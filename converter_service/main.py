import os
import sys
import json
import logging
import pika
from minio import Minio
from moviepy.editor import VideoFileClip
from pythonjsonlogger import jsonlogger
import tempfile

# --- Logging Setup ---
logger = logging.getLogger("converter_service")
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(fmt='%(asctime)s %(levelname)s %(name)s %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# --- Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "psycho-rabbitmq")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "psycho-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
AUDIO_BUCKET = "audio"

# --- MinIO Client ---
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure output bucket exists
if not minio_client.bucket_exists(AUDIO_BUCKET):
    minio_client.make_bucket(AUDIO_BUCKET)

def process_video(ch, method, properties, body):
    """
    This function runs whenever a message arrives.
    """
    message = json.loads(body)
    video_id = message['video_id']
    bucket_name = message['bucket']
    
    logger.info(f"Received job for: {video_id}")

    # Create temporary paths
    # We use tempfile to ensure cleanup (though in Docker, containers are ephemeral)
    temp_video_path = f"/tmp/{video_id}"
    temp_audio_path = f"/tmp/{os.path.splitext(video_id)[0]}.mp3"

    try:
        # 1. Download Video
        minio_client.fget_object(bucket_name, video_id, temp_video_path)
        logger.info("Video downloaded")

        # 2. Extract Audio
        clip = VideoFileClip(temp_video_path)
        clip.audio.write_audiofile(temp_audio_path, logger=None) # logger=None silences moviepy's own logs
        clip.close()
        logger.info("Audio extracted to mp3")

        # 3. Upload Audio
        mp3_filename = os.path.basename(temp_audio_path)
        minio_client.fput_object(AUDIO_BUCKET, mp3_filename, temp_audio_path)
        logger.info(f"Audio uploaded to bucket: {AUDIO_BUCKET}")

        # 4. Publish to Next Queue
        next_message = {
            "video_id": video_id,
            "audio_id": mp3_filename,
            "bucket": AUDIO_BUCKET
        }
        
        # We use the same channel to publish the next step
        ch.basic_publish(
            exchange='',
            routing_key='audio_processing',
            body=json.dumps(next_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logger.info("Published to audio_processing")

        # 5. Acknowledge (Tell RabbitMQ we are done so it removes message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Failed to process {video_id}: {str(e)}")
        # Ideally, we would NACK here so it retries, but for now we ACK to avoid infinite loops of errors
        ch.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        # Cleanup local files to save space
        if os.path.exists(temp_video_path): os.remove(temp_video_path)
        if os.path.exists(temp_audio_path): os.remove(temp_audio_path)


def main():
    logger.info("Starting Converter Service...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declare queues (Idempotent: creates if not exists)
    channel.queue_declare(queue='video_processing', durable=True)
    channel.queue_declare(queue='audio_processing', durable=True)

    # Tell RabbitMQ to send 1 message at a time to this worker
    channel.basic_qos(prefetch_count=1)

    #TODO: check if enters infinite loop
    channel.basic_consume(queue='video_processing', on_message_callback=process_video)

    logger.info("Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()