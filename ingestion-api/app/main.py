import gzip
import json
import uuid
import asyncio
import io
import os
import asyncpg
import hashlib
import requests

from datetime import datetime
from fastapi import FastAPI, Request, Header, HTTPException
from minio import Minio
from pydantic import BaseModel
from typing import Dict, Any, Optional


# -----------------------------
# Pydantic payload schema
# -----------------------------
class TelemetryPayload(BaseModel):
    device: Dict[str, Any]
    battery: Dict[str, Any]
    app: Dict[str, Any]
    session: Dict[str, Any]
    sensor: Optional[Dict[str, Any]] = {}
    network: Optional[Dict[str, Any]] = {}


# -----------------------------
# Tokenization helper
# -----------------------------
def tokenize(value: str) -> str:
    """
    Deterministic tokenization using SHA256
    Same input -> same token
    """
    return "tok_" + hashlib.sha256(value.encode()).hexdigest()


# -----------------------------
# Environment variables
# -----------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "telemetrydb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "telemetry")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "telemetry")

MINIO_BUCKET = "telemetry"

AIRFLOW_URL = "http://telemetry_airflow:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = "admin"

db_pool = None


# -----------------------------
# Trigger Airflow DAG
# -----------------------------
def trigger_airflow_dag(dag_id: str):
    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"

    response = requests.post(
        url,
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        json={
            "dag_run_id": f"flutter_{datetime.utcnow().isoformat()}"
        },
        timeout=10
    )

    if response.status_code not in (200, 201):
        raise Exception(f"Airflow trigger failed: {response.text}")


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Telemetry Ingestion API")


# -----------------------------
# MinIO client
# -----------------------------
minio_client = Minio(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)


# -----------------------------
# Startup event
# -----------------------------
@app.on_event("startup")
async def startup_event():
    global db_pool

    # ---- MinIO ----
    for i in range(10):
        try:
            if not minio_client.bucket_exists(MINIO_BUCKET):
                minio_client.make_bucket(MINIO_BUCKET)
            print("✅ MinIO bucket ready")
            break
        except Exception as e:
            print(f"⏳ Waiting for MinIO... attempt {i + 1}: {e}")
            await asyncio.sleep(3)
    else:
        raise RuntimeError("❌ Could not connect to MinIO")

    # ---- Postgres ----
    try:
        db_pool = await asyncpg.create_pool(
            host=POSTGRES_HOST,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB
        )
        print("✅ Postgres connection pool ready")
    except Exception as e:
        raise RuntimeError(f"❌ Could not connect to Postgres: {e}")


# -----------------------------
# Ingest endpoint
# -----------------------------
@app.post("/ingest")
async def ingest(
    request: Request,
    x_api_key: str = Header(None)
):
    # ---- API key validation ----
    if x_api_key != "test123":
        raise HTTPException(status_code=401, detail="Invalid API Key")

    # ---- Read raw body ----
    body = await request.body()

    # ---- Handle gzip or plain JSON ----
    try:
        raw_data = gzip.decompress(body).decode()
    except Exception:
        raw_data = body.decode()

    # ---- JSON parse ----
    try:
        parsed_json = json.loads(raw_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

    # ---- Pydantic validation ----
    try:
        payload = TelemetryPayload(**parsed_json)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    obj = payload.dict()

    # -----------------------------
    # Tokenize sensitive fields
    # -----------------------------
    if obj.get("network"):
        network_str = json.dumps(obj["network"], sort_keys=True)
        obj["network_token"] = tokenize(network_str)
        del obj["network"]

    # -----------------------------
    # Store raw telemetry in MinIO
    # -----------------------------
    object_name = f"telemetry/{uuid.uuid4()}.json"
    tokenized_data = json.dumps(obj)

    try:
        minio_client.put_object(
            MINIO_BUCKET,
            object_name,
            io.BytesIO(tokenized_data.encode()),
            length=len(tokenized_data)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload to MinIO: {str(e)}"
        )

    # -----------------------------
    # Register file as UNPROCESSED
    # -----------------------------
    try:
        async with db_pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO telemetry_files (object_name, processed)
                VALUES ($1, FALSE)
                ON CONFLICT (object_name) DO NOTHING
                """,
                object_name
            )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to register telemetry file: {str(e)}"
        )

    # -----------------------------
    # Insert into Postgres staging
    # -----------------------------
    try:
        async with db_pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO telemetry_staging (id, source, payload)
                VALUES ($1, $2, $3::jsonb)
                """,
                str(uuid.uuid4()),
                "mobile-sdk",
                json.dumps(obj)
            )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to insert into Postgres: {str(e)}"
        )

    # ---- Logging ----
    print(
        f"📥 Telemetry ingested | session={obj['session']['id']} "
        f"| device={obj['device'].get('model')}"
    )

    return {"status": "ok", "object_name": object_name}


# -----------------------------
# Start telemetry pipeline
# -----------------------------
@app.post("/start-telemetry")
async def start_telemetry():
    print("🔥 START TELEMETRY ENDPOINT HIT")

    try:
        trigger_airflow_dag("telemetry_raw_loader")
        print("🚀 telemetry_raw_loader DAG triggered")
    except Exception as e:
        print("❌ Failed to trigger DAG:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "status": "pipeline_started",
        "dag": "telemetry_raw_loader"
    }
