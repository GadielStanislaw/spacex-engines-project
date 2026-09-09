"""Lambda handler for batch ingestion: pulls launches from Launch Library 2
(one page per rocket family) plus the static engine specs table, landing both
as raw JSON in S3.

Self-contained like consumer.py (see infra/deploy_ingest_lambda.py) -- engine
specs are duplicated here rather than imported from engine_specs.py, so the
deploy zip stays this one file plus vendored `requests`. Keep in sync with
src/batch/engine_specs.py (used by the local CLI ingester, ingest_batch.py).

If the upstream API rate-limits us mid-run, this returns early with whatever
it already fetched rather than sleeping through the (sometimes ~1hr)
Retry-After window -- Lambda has a hard 15-minute cap, so blocking that long
would just waste the invocation. Re-invoke later (manually, or on an
EventBridge schedule in production) to pick up the rest; already-ingested
families are skipped via an S3 existence check either way.
"""
import json
import os
import time

import boto3
import requests
from botocore.exceptions import ClientError

S3_BUCKET = os.environ["S3_BUCKET"]
LL2_BASE = "https://ll.thespacedevs.com/2.2.0/launch/"
PAGE_LIMIT = 100

ENGINE_SPECS = [
    {"rocket_family": "Falcon 9", "engine_name": "Merlin 1D", "manufacturer": "SpaceX",
     "engine_count": 9, "propellant_type": "LOX/RP-1", "thrust_sl_kN": 845, "thrust_vac_kN": 914,
     "isp_sl_s": 282, "isp_vac_s": 311},
    {"rocket_family": "Falcon Heavy", "engine_name": "Merlin 1D", "manufacturer": "SpaceX",
     "engine_count": 27, "propellant_type": "LOX/RP-1", "thrust_sl_kN": 845, "thrust_vac_kN": 914,
     "isp_sl_s": 282, "isp_vac_s": 311},
    {"rocket_family": "Saturn V", "engine_name": "Rocketdyne F-1", "manufacturer": "Rocketdyne",
     "engine_count": 5, "propellant_type": "LOX/RP-1", "thrust_sl_kN": 6770, "thrust_vac_kN": 7770,
     "isp_sl_s": 263, "isp_vac_s": 304},
    {"rocket_family": "Space Shuttle", "engine_name": "RS-25 (SSME)", "manufacturer": "Rocketdyne",
     "engine_count": 3, "propellant_type": "LOX/LH2", "thrust_sl_kN": 1860, "thrust_vac_kN": 2279,
     "isp_sl_s": 366, "isp_vac_s": 452},
    {"rocket_family": "Soyuz", "engine_name": "RD-108A", "manufacturer": "NPO Energomash",
     "engine_count": 1, "propellant_type": "LOX/RP-1", "thrust_sl_kN": 839, "thrust_vac_kN": 990,
     "isp_sl_s": 257, "isp_vac_s": 320},
    {"rocket_family": "Atlas V", "engine_name": "RD-180", "manufacturer": "NPO Energomash",
     "engine_count": 1, "propellant_type": "LOX/RP-1", "thrust_sl_kN": 3830, "thrust_vac_kN": 4152,
     "isp_sl_s": 311, "isp_vac_s": 338},
    {"rocket_family": "Delta IV Heavy", "engine_name": "RS-68A", "manufacturer": "Aerojet Rocketdyne",
     "engine_count": 3, "propellant_type": "LOX/LH2", "thrust_sl_kN": 3137, "thrust_vac_kN": 3560,
     "isp_sl_s": 365, "isp_vac_s": 412},
    {"rocket_family": "Ariane 5", "engine_name": "Vulcain 2", "manufacturer": "ArianeGroup",
     "engine_count": 1, "propellant_type": "LOX/LH2", "thrust_sl_kN": 960, "thrust_vac_kN": 1390,
     "isp_sl_s": 318, "isp_vac_s": 431},
    {"rocket_family": "Electron", "engine_name": "Rutherford", "manufacturer": "Rocket Lab",
     "engine_count": 9, "propellant_type": "LOX/RP-1", "thrust_sl_kN": 22.7, "thrust_vac_kN": 25.8,
     "isp_sl_s": 303, "isp_vac_s": 311},
]

s3 = boto3.client("s3")


def slugify(name: str) -> str:
    return name.lower().replace(" ", "-")


def s3_key_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def fetch_one_page(family: str):
    """Single page (up to 100 launches), no retry-on-429 -- see module docstring."""
    resp = requests.get(LL2_BASE, params={"search": family, "limit": PAGE_LIMIT}, timeout=20)
    if resp.status_code == 429:
        return None, resp.headers.get("Retry-After")
    resp.raise_for_status()
    return resp.json()["results"], None


def handler(event, context):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="raw/engine_specs.json",
        Body=json.dumps(ENGINE_SPECS, indent=2).encode("utf-8"),
    )

    ingested, skipped = [], []
    for spec in ENGINE_SPECS:
        family = spec["rocket_family"]
        key = f"raw/launchlibrary2/{slugify(family)}.json"

        if s3_key_exists(key):
            skipped.append(family)
            continue

        launches, retry_after = fetch_one_page(family)
        if launches is None:
            return {
                "ingested": ingested,
                "skipped": skipped,
                "stopped_early": True,
                "reason": f"rate limited, retry after {retry_after}s",
            }

        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(launches).encode("utf-8"))
        ingested.append({"family": family, "count": len(launches)})
        time.sleep(1)

    return {"ingested": ingested, "skipped": skipped, "stopped_early": False}
