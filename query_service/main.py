import os
import sys
import logging
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pythonjsonlogger import jsonlogger

# --- Logging Setup ---
logger = logging.getLogger("query_service")
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(fmt='%(asctime)s %(levelname)s %(name)s %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

app = FastAPI()

# --- Configuration ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://psycho-mongo:27017")

# --- Database Connection ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['psychology_db']
analysis_collection = db['analyses']

@app.get("/analyses")
def list_analyses():
    """Returns a list of all videos that have been analyzed."""
    # We only return the ID and Summary to keep the list lightweight
    cursor = analysis_collection.find({}, {"video_id": 1, "analysis.summary": 1, "_id": 0})
    results = list(cursor)
    logger.info(f"Listed {len(results)} analyses")
    return results

@app.get("/analyses/{video_id}")
def get_analysis(video_id: str):
    """Returns the full analysis for a specific video."""
    result = analysis_collection.find_one({"video_id": video_id}, {"_id": 0})
    
    if result:
        logger.info(f"Retrieved analysis for {video_id}")
        return result
    else:
        logger.warning(f"Analysis not found for {video_id}")
        raise HTTPException(status_code=404, detail="Analysis not found")

@app.get("/health")
async def health_check():
    return {"status": "ok"}