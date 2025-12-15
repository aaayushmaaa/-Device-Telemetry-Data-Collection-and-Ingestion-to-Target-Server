import gzip
import json
import uuid
import asyncio
import io
import os
import hashlib

from fastapi import FastAPI, Request, Header, HTTPException
from minio import Minio
import asyncpg


# -----------------------------
# Tokenization helper
# -----------------------------
def tokenize(value: str) -> str:
    """
    Minimal deterministic tokenization using SHA256.
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

db_pool = None


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI()


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

    # --- MinIO ---
    for i in range(10):
        try:
            if not minio_client.bucket_exists(MINIO_BUCKET):
                minio_client.make_bucket(MINIO_BUCKET)
            print("✅ MinIO bucket ready")
            break
        except Exception as e:
            print(f"Waiting for MinIO... attempt {i+1}: {e}")
            await asyncio.sleep(3)
    else:
        raise RuntimeError("❌ Could not connect to MinIO")

    # --- Postgres ---
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
async def ingest(request: Request, x_api_key: str = Header(None)):
    if x_api_key != "test123":
        raise HTTPException(status_code=401, detail="Invalid API Key")

    body = await request.body()

    # Handle compressed / uncompressed JSON
    try:
        raw_data = gzip.decompress(body).decode()
    except Exception:
        raw_data = body.decode()

    try:
        obj = json.loads(raw_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

    # -----------------------------
    # Tokenize sensitive fields
    # -----------------------------
    if isinstance(obj, dict):
        if "network" in obj:
            network_str = json.dumps(obj["network"])
            obj["network_token"] = tokenize(network_str)
            del obj["network"]

    tokenized_data = json.dumps(obj)

    # -----------------------------
    # Store raw telemetry in MinIO
    # -----------------------------
    object_name = f"telemetry/{uuid.uuid4()}.json"

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

    print("📥 Telemetry ingested:", obj)
    return {"status": "ok"}
