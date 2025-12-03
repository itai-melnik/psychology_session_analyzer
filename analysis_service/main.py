import os
import sys
import json
import hashlib
import logging
import pika
import redis
from pymongo import MongoClient
from openai import OpenAI
from pythonjsonlogger import jsonlogger

# --- Logging Setup ---
logger = logging.getLogger("analysis_service")
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(fmt='%(asctime)s %(levelname)s %(name)s %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# --- Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "psycho-rabbitmq")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://psycho-mongo:27017")
REDIS_HOST = os.getenv("REDIS_HOST", "psycho-redis")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- Clients ---
# 1. Redis (Cache)
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

# 2. MongoDB (Database)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['psychology_db']
analysis_collection = db['analyses']

# 3. OpenAI (Intelligence)
client = OpenAI(api_key=OPENAI_API_KEY)

def generate_hash(text):
    """Creates a unique fingerprint for a text string."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def analyze_transcript(transcript_text):
    """Calls OpenAI to analyze the session."""
    system_prompt = """
    You are an expert Psychology Supervisor. 
    1. Analyze the transcript. Identify who is the Therapist and who is the Patient.
    2. For every sentence, tag the 'topic' and the 'sentiment' (positive/negative/neutral).
    3. Output PURE JSON format with this structure:
    {
        "summary": "...",
        "participants": {"speaker_A": "Therapist", "speaker_B": "Patient"},
        "sentences": [
            {"speaker": "A", "text": "...", "topic": "...", "sentiment": "..."}
        ]
    }
    """
    
    response = client.chat.completions.create(
        model="gpt-5-mini", # TODO: check if this is the correct model
        response_format={ "type": "json_object" }, # Crucial: Force JSON output
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": transcript_text}
        ]
    )
    
    return json.loads(response.choices[0].message.content)

def process_analysis(ch, method, properties, body):
    message = json.loads(body)
    video_id = message['video_id']
    transcript = message['transcript']
    
    logger.info(f"Processing analysis for: {video_id}")

    try:
        # 1. Check Cache (Redis)
        transcript_hash = generate_hash(transcript)
        cached_result = redis_client.get(transcript_hash)

        if cached_result:
            logger.info("CACHE HIT! Using existing analysis.")
            analysis_data = json.loads(cached_result)
            # Add metadata just in case
            analysis_data['source'] = 'cache'
        else:
            logger.info("Cache Miss. Calling OpenAI...")
            analysis_data = analyze_transcript(transcript)
            analysis_data['source'] = 'openai'
            
            # Save to Cache (Expire in 24 hours)
            redis_client.setex(transcript_hash, 86400, json.dumps(analysis_data))

        # 2. Save to MongoDB
        # We add the video_id so we can look it up later
        document = {
            "video_id": video_id,
            "analysis": analysis_data,
            "transcript_hash": transcript_hash,
            "created_at": str(properties.headers) if properties.headers else None
        }
        
        # Upsert: Update if exists, Insert if new
        analysis_collection.update_one(
            {"video_id": video_id}, 
            {"$set": document}, 
            upsert=True
        )
        logger.info(f"Saved analysis to MongoDB for {video_id}")

        # 3. Acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Analysis Failed: {str(e)}")
        # NACK ensures the message isn't lost, but be careful of infinite loops
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) 

def main():
    logger.info("Starting Analysis Service...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='analysis_processing', durable=True)
    channel.basic_qos(prefetch_count=1)
    
    channel.basic_consume(queue='analysis_processing', on_message_callback=process_analysis)
    channel.start_consuming()

if __name__ == "__main__":
    main()