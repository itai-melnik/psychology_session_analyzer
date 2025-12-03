import os
import json
import logging
from fastapi import FastAPI, UploadFile, HTTPException
from minio import Minio
import pika
import sys
from pythonjsonlogger import jsonlogger

# --- Enhanced JSON Logging ---
logger = logging.getLogger("upload_service")
logHandler = logging.StreamHandler(sys.stdout) # Write to stdout (not stderr)
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(name)s %(message)s'
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)
# -----------------------------

app = FastAPI()

# --- Configuration ---
# We read these from environment variables so we can change them in docker-compose
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "psycho-rabbitmq")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "psycho-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "videos"

# --- Clients ---

# 1. Initialize MinIO Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # We are using HTTP, not HTTPS internally # TODO: check if we need to use HTTPS
)

# 2. Helper to get RabbitMQ Channel
def get_rabbitmq_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    # Ensure queue exists so we don't crash if this runs before the consumer
    channel.queue_declare(queue='video_processing', durable=True)
    return connection, channel

# --- Ensure Bucket Exists on Startup ---
@app.on_event("startup")
def startup_event():
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        logger.info(f"Created bucket: {BUCKET_NAME}")

# --- API Endpoints ---

@app.post("/upload")
async def upload_video(file: UploadFile):
    try:
        # 1. Upload file to MinIO
        # We use the file's original filename as the object name
        file_size = os.fstat(file.file.fileno()).st_size
        
        minio_client.put_object(
            BUCKET_NAME,
            file.filename,
            file.file,
            file_size,
            content_type=file.content_type
        )
        logger.info(f"Uploaded {file.filename} to MinIO")

        # 2. Publish message to RabbitMQ
        message = {
            "video_id": file.filename,  # Using filename as ID for simplicity
            "file_path": file.filename,
            "bucket": BUCKET_NAME
        }
        
        connection, channel = get_rabbitmq_channel()
        channel.basic_publish(
            exchange='',
            routing_key='video_processing',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        connection.close()
        logger.info(f"Published message to RabbitMQ: {message}")

        return {"message": "Video uploaded and processing started", "video_id": file.filename}

    except Exception as e:
        logger.error(f"Error processing upload: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))