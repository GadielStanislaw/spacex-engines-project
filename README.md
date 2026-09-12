# Spacecraft Engines Data Pipeline

Proyecto final del curso de Ingeniería de Datos. Pipeline de datos sobre motores de cohetes espaciales, con dos flujos independientes — **batch** (histórico de lanzamientos) y **streaming en tiempo real** (telemetría simulada de motores) — desplegados sobre AWS real.

**Autores:** Stan Mora, Tais Rodriguez

## Arquitectura

Dos flujos paralelos que convergen en un único dashboard:

**Batch:** Launch Library 2 API + tabla de especificaciones de motores → AWS Lambda (`batch-ingest`) → S3 (`raw/`) → `transform_batch.py` (join + limpieza) → S3 (`curated/`, Parquet) → `train_model.py` (Logistic Regression + Random Forest) → S3 (`models/`).

**Streaming:** `producer.py` (simulador de telemetría, basado en física real) → Amazon SQS (`engine-telemetry`) → AWS Lambda (`engine-telemetry-consumer`, disparada automáticamente vía event source mapping) → DynamoDB (`latest_reading`, estado actual por motor) + S3 (`raw/telemetry/`, histórico completo).

**Dashboard:** `src/dashboard/app.py` (Streamlit) lee de ambos flujos — Parquet curado + métricas del modelo desde S3, y la tabla en vivo desde DynamoDB.

El mismo código corre localmente (contra [Floci](https://github.com/floci-io/floci-cli), un emulador de AWS tipo LocalStack, vía Docker) o contra AWS real — solo cambia la variable de entorno `AWS_ENV` (`local` | `aws`) en `.env`.

## Fuentes de datos

1. **Launch Library 2 API** (`ll.thespacedevs.com`) — histórico real de lanzamientos, multi-agencia (SpaceX, NASA, Roscosmos, ESA, Rocket Lab, Blue Origin), gratuita, sin API key.
2. **`src/batch/engine_specs.py`** — tabla estática con especificaciones reales de 10 motores (empuje, Isp, tipo de propelente): Merlin 1D, Rocketdyne F-1, RS-25, RD-108A, RD-180, RS-68A, Vulcain 2, Rutherford, BE-4. No es de una API — no existe ninguna gratuita con ese nivel de detalle por motor.

## Estructura del repo

```
src/
  batch/         ingesta (ingest_batch.py, ingest_lambda.py), transformación, specs de motores
  streaming/     producer.py, consumer.py (Lambda), validate_anomaly_detector.py
  ml/            train_model.py
  dashboard/     app.py (Streamlit)
  config.py      toggle AWS_ENV local/aws, credenciales/endpoints
infra/
  setup.py               crea bucket S3, tabla DynamoDB, cola SQS
  deploy_lambda.py        despliega la Lambda de streaming
  deploy_ingest_lambda.py despliega la Lambda de ingesta batch
tests/
  test_infra_smoke.py    smoke test de S3/DynamoDB/SQS
.github/workflows/
  deploy-aws.yml          despliegue completo a AWS real (manual)
  run-producer.yml        genera telemetría de prueba en AWS real (manual)
docker-compose.yml        entorno local (Floci)
```

## Cómo ejecutar localmente

Requiere Docker y Python 3.11+.

```bash
pip install -r requirements.txt
cp .env.example .env          # AWS_ENV=local por defecto

docker compose up -d          # levanta Floci (emulador AWS local)
python infra/setup.py         # crea bucket S3, tabla DynamoDB, cola SQS

# --- flujo batch ---
python -m src.batch.ingest_batch
python -m src.batch.transform_batch
python -m src.ml.train_model

# --- flujo streaming ---
python infra/deploy_lambda.py
python -m src.streaming.producer --ticks 15 --interval 1

# --- validar el detector de anomalías ---
python -m src.streaming.validate_anomaly_detector

# --- dashboard ---
streamlit run src/dashboard/app.py
```

El dashboard queda disponible en `http://localhost:8501`. Para ver los recursos de Floci en una UI (S3, DynamoDB, SQS), levantar además `docker compose --profile ui up -d` y abrir `http://localhost:4501`.

## Despliegue a AWS real

Vía GitHub Actions (`Actions` → seleccionar workflow → `Run workflow`), usando las credenciales de un usuario IAM configuradas como secrets del repo (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`):

- **`Deploy to real AWS`** — crea la infraestructura, despliega ambas Lambdas, ingiere datos batch, transforma y entrena el modelo.
- **`Run streaming producer (real AWS)`** — genera telemetría de prueba contra la cola SQS real, poblando DynamoDB con datos en vivo.

## Resultados

- **930 lanzamientos reales** ingeridos y curados, across 9 familias de cohetes con datos que hacen match limpio (Atlas V se excluye explícitamente por un problema de matching en los datos fuente, documentado en `transform_batch.py`).
- **Modelo de predicción de éxito/fallo por lanzamiento:** señal individual débil (ROC-AUC ~0.64, la variable `year` domina la importancia de features) — resultado reportado con honestidad metodológica, no exagerado. El hallazgo más sólido es descriptivo: la tasa de éxito varía genuinamente por familia de cohete.
- **Detector de anomalías en streaming:** validado contra ground truth inyectado por el propio simulador — 95% recall / 76% precision.

## Tecnologías

AWS Lambda (×2, funciones reales desplegadas), Amazon SQS, Amazon DynamoDB, Amazon S3, IAM, GitHub Actions (CI/CD), Floci (entorno local), Python (boto3, pandas, scikit-learn, Streamlit), Parquet.
