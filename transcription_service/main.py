import os
import sys
import json
import time
import logging
import requests
import pika
from minio import Minio
from pythonjsonlogger import jsonlogger

# --- Logging Setup ---
logger = logging.getLogger("transcription_service")
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
ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")

# --- Clients ---
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

headers = {
    "authorization": ASSEMBLYAI_API_KEY,
    "content-type": "application/json"
}

def upload_to_assemblyai(file_path):
    """Uploads the local file to AssemblyAI so they can access it."""
    def read_file(path):
        with open(path, 'rb') as f:
            while True:
                data = f.read(5242880) # Read in 5MB chunks
                if not data:
                    break
                yield data

    upload_response = requests.post(
        'https://api.assemblyai.com/v2/upload',
        headers=headers,
        data=read_file(file_path)
    )
    upload_url = upload_response.json()['upload_url']
    return upload_url

def transcribe_audio(ch, method, properties, body):
    message = json.loads(body)
    video_id = message['video_id']
    audio_id = message['audio_id']
    bucket_name = message['bucket']

    logger.info(f"Starting transcription for {video_id}")
    
    local_filename = f"/tmp/{audio_id}"

    try:
        # 1. Download MP3 from MinIO
        minio_client.fget_object(bucket_name, audio_id, local_filename)
        
        # 2. Upload to AssemblyAI
        audio_url = upload_to_assemblyai(local_filename)
        logger.info("Uploaded to AssemblyAI")

        # 3. Start Transcription (with Speaker Labels)
        endpoint = "https://api.assemblyai.com/v2/transcript"
        json_data = {
            "audio_url": audio_url,
            "speaker_labels": True  # <--- Crucial for Therapist vs Patient
        }
        response = requests.post(endpoint, json=json_data, headers=headers)
        transcript_id = response.json()['id']
        logger.info(f"Transcription started. ID: {transcript_id}")

        # 4. Poll for Completion
        while True:
            polling_endpoint = f"https://api.assemblyai.com/v2/transcript/{transcript_id}"
            polling_response = requests.get(polling_endpoint, headers=headers)
            status = polling_response.json()['status']

            if status == 'completed':
                result = polling_response.json()
                logger.info("Transcription completed!")
                
                # 5. Publish Result
                # We send the WHOLE result (including speaker utterances) to the next service
                next_message = {
                    "video_id": video_id,
                    "transcript": result['text'],
                    "utterances": result['utterances'] # List of {speaker: "A", text: "..."}
                }
                
                ch.basic_publish(
                    exchange='',
                    routing_key='analysis_processing',
                    body=json.dumps(next_message),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
                break
            
            elif status == 'error':
                logger.error("AssemblyAI failed")
                ch.basic_ack(delivery_tag=method.delivery_tag) # Ack to remove bad job
                break
            
            else:
                time.sleep(3) # Wait 3 seconds before checking again

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        if os.path.exists(local_filename): os.remove(local_filename)

def main():
    logger.info("Starting Transcription Service...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='audio_processing', durable=True)
    channel.queue_declare(queue='analysis_processing', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='audio_processing', on_message_callback=transcribe_audio)
    channel.start_consuming()

if __name__ == "__main__":
    main()