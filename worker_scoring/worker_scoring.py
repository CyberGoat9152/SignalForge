import pika
import json
import yaml
import time
import numpy as np
from datetime import datetime, timezone
import sys
import logging
from shared_utils import (
    get_mongo_client, log_execution,
    get_job, update_job_stage, update_job_scores, update_job_status, set_job_error
)

sys.path.append('..')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("scoring_worker")

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]

def normalize(values):
    if not values or len(values) == 0:
        return []
    arr = np.array(values)
    min_val = arr.min()
    max_val = arr.max()
    if max_val - min_val == 0:
        return [0.5] * len(values)
    return ((arr - min_val) / (max_val - min_val)).tolist()

def score_segments(job):
    audio_features = job.get("features", {}).get("audio", [])
    metadata = job.get("metadata", {}).get("public", {})
    
    if not audio_features:
        return []
    
    rms_values = [f["rms"] for f in audio_features]
    zcr_values = [f["zcr"] for f in audio_features]
    pitch_values = [f["pitch"] for f in audio_features]
    
    norm_rms = normalize(rms_values)
    norm_zcr = normalize(zcr_values)
    norm_pitch = normalize(pitch_values)
    
    engagement_weight = 1.0
    if metadata:
        engagement_score = metadata.get("engagement_score", 0)
        if engagement_score > 0.01:
            engagement_weight = 1.5
        elif engagement_score > 0.005:
            engagement_weight = 1.2
    
    segments = []
    for i, feature in enumerate(audio_features):
        audio_score = (
            norm_rms[i] * 0.4 +
            norm_zcr[i] * 0.3 +
            norm_pitch[i] * 0.3
        ) * engagement_weight
        
        segments.append({
            "start": feature["start"],
            "end": feature["end"],
            "audio_score": audio_score,
            "rms": feature["rms"],
            "zcr": feature["zcr"],
            "pitch": feature["pitch"]
        })
    
    # Smooth scores with moving window
    window_size = 3
    smoothed_segments = []
    for i in range(len(segments)):
        start_idx = max(0, i - window_size // 2)
        end_idx = min(len(segments), i + window_size // 2 + 1)
        window_scores = [segments[j]["audio_score"] for j in range(start_idx, end_idx)]
        smoothed_score = np.mean(window_scores)
        
        segment = segments[i].copy()
        segment["final_score"] = smoothed_score
        smoothed_segments.append(segment)
    
    ranked_segments = sorted(smoothed_segments, key=lambda x: x["final_score"], reverse=True)
    
    return ranked_segments

def callback(ch, method, properties, body):
    start_time = time.time()
    msg = json.loads(body)
    
    if msg.get("stage") != "scoring":
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    
    try:
        job = get_job(mongo_db, job_id)
        if not job:
            raise Exception(f"Job {job_id} not found in MongoDB")
        
        update_job_stage(mongo_db, job_id, "scoring", "running")

        ranked_segments = score_segments(job)

        scores_data = {
            "segments": ranked_segments[:50]
        }
        update_job_scores(mongo_db, job_id, scores_data)
        update_job_stage(mongo_db, job_id, "scoring", "done")
        update_job_status(mongo_db, job_id, "completed")

        execution_time = time.time() - start_time
        
        top_score = ranked_segments[0]["final_score"] if ranked_segments else 0
        avg_score = np.mean([s["final_score"] for s in ranked_segments]) if ranked_segments else 0
        
        log_execution(mongo_db, "scoring", {
            "job_id": job_id,
            "segment_count": len(ranked_segments),
            "top_score": float(top_score),
            "avg_score": float(avg_score),
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        logger.info(f"[Worker-Scoring] [{job_id}] <OK> Scoring completed for job ")
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        logger.error(f"[Worker-Scoring] [{job_id}] <Failed> Scoring failed for job see mongodb for logging")
        
        log_execution(mongo_db, "scoring", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        set_job_error(mongo_db, job_id, "scoring", str(e))
        
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

print("[Worker-Scoring] Scoring worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
print("[Worker-Scoring] Scoring worker starting...OK")
print(f"[Worker-Scoring] Listening on {LISTEN_QUEUE}")
ch.start_consuming()
