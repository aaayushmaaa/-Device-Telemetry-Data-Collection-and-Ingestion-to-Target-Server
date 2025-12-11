import gzip
import json
from fastapi import FastAPI, Request, Header, HTTPException
from minio import Minio
import uuid
import asyncio
import io

app = FastAPI()

minio_client = Minio(
    endpoint="minio:9000",  # Docker service name
    access_key="admin",
    secret_key="admin123",
    secure=False
)

@app.on_event("startup")
async def startup_event():
    # Retry a few times in case MinIO isn't ready
    for i in range(10):
        try:
            if not minio_client.bucket_exists("telemetry"):
                minio_client.make_bucket("telemetry")
            print("MinIO bucket ready")
            return
        except Exception as e:
            print(f"Waiting for MinIO... attempt {i+1}: {e}")
            await asyncio.sleep(3)
    raise RuntimeError("Could not connect to MinIO at startup")

@app.post("/ingest")
async def ingest(request: Request, x_api_key: str = Header(None)):
    if x_api_key != "test123":
        raise HTTPException(401, "Invalid API Key")

    body = await request.body()

    # handle compressed or uncompressed JSON
    try:
        data = gzip.decompress(body).decode()
    except Exception:
        data = body.decode()

    try:
        obj = json.loads(data)
    except json.JSONDecodeError as e:
        raise HTTPException(400, f"Invalid JSON: {str(e)}")

    object_name = f"telemetry/{uuid.uuid4()}.json"

    try:
        minio_client.put_object(
            "telemetry",
            object_name,
            io.BytesIO(data.encode()),
            length=len(data)
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to upload to MinIO: {str(e)}")

    print("Telemetry ingested:", obj)
    return {"status": "ok"}
