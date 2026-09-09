"""Single-page dashboard: batch analytics + model results (top) and the
live streaming telemetry panel (bottom), reading from whichever AWS_ENV the
.env is pointed at (Floci locally, real AWS on demo day -- no code change).

Usage: streamlit run src/dashboard/app.py
"""
import io
import json
import sys
import time
from pathlib import Path

import boto3
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import S3_BUCKET, DYNAMODB_TABLE, boto3_kwargs

st.set_page_config(page_title="Spacecraft Engines Dashboard", layout="wide")


@st.cache_resource
def get_clients():
    kwargs = boto3_kwargs()
    return boto3.client("s3", **kwargs), boto3.resource("dynamodb", **kwargs)


def read_parquet(s3, key: str) -> pd.DataFrame:
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def read_json(s3, key: str) -> dict:
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


s3, dynamodb = get_clients()

st.title("Spacecraft Engines — Batch + Streaming Dashboard")

# ---------------------------------------------------------------- BATCH ----
st.header("Batch: launch history & model")

engines_df = read_parquet(s3, "curated/engines.parquet")
launches_df = read_parquet(s3, "curated/launches.parquet")
merged = launches_df.merge(engines_df, on="rocket_family", how="inner")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total launches", len(merged))
col2.metric("Overall success rate", f"{merged['success'].mean():.1%}")
col3.metric("Engine families", merged["rocket_family"].nunique())
col4.metric("Failures observed", int((~merged["success"]).sum()))

left, right = st.columns(2)

with left:
    st.subheader("Success rate by rocket family")
    by_family = (
        merged.groupby(["rocket_family", "engine_name"])["success"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )
    by_family["label"] = by_family["rocket_family"] + " (" + by_family["engine_name"] + ")"
    st.bar_chart(by_family.set_index("label")["success"], height=320)
    st.caption("Descriptive result: reliability genuinely varies by rocket family/era.")

with right:
    st.subheader("Model: feature importance")
    try:
        metrics = read_json(s3, "models/metrics.json")
        importance_df = pd.DataFrame(metrics["feature_importance"]).set_index("feature")
        st.bar_chart(importance_df["importance"], height=320)

        best = metrics["best_model"]
        roc_auc = metrics[best]["roc_auc"]
        st.warning(
            f"Honest caveat: best model ({best}) gets ROC-AUC {roc_auc:.2f} at "
            f"predicting individual launch failure -- only ~{int((~merged['success']).sum())} "
            f"real failures exist in the data, and `year` alone dominates feature "
            f"importance ({importance_df['importance'].iloc[0] if 'year' in importance_df.index else 0:.0%} "
            f"if it's `year`). Reliability is better explained by family/era than by "
            f"any single engine spec -- see the chart on the left for the stronger, "
            f"descriptive result."
        )
    except s3.exceptions.NoSuchKey:
        st.info("Run `python -m src.ml.train_model` first to populate this section.")

# ------------------------------------------------------------- STREAMING ---
st.header("Streaming: live engine telemetry")

table = dynamodb.Table(DYNAMODB_TABLE)
items = table.scan()["Items"]

if items:
    live_df = pd.DataFrame(items)
    display_cols = ["engine_id", "thrust", "chamber_pressure", "temperature",
                     "vibration", "fuel_flow", "anomaly_flag", "anomaly_reason"]
    live_df = live_df[[c for c in display_cols if c in live_df.columns]].sort_values("engine_id")

    n_anomalous = int(live_df["anomaly_flag"].sum()) if "anomaly_flag" in live_df else 0
    st.metric("Engines currently anomalous", n_anomalous)

    def highlight_anomaly(row):
        color = "background-color: #f8d7da" if row.get("anomaly_flag") else ""
        return [color] * len(row)

    st.dataframe(live_df.style.apply(highlight_anomaly, axis=1), use_container_width=True)
else:
    st.info("No telemetry yet -- run `python -m src.streaming.producer` to generate some.")

st.caption(f"Auto-refreshing every 3 seconds. Last updated: {time.strftime('%H:%M:%S')}")
time.sleep(3)
st.rerun()
