import pika
import json
import yaml
import redis
import time
import numpy as np
from datetime import datetime, timezone
import sys
sys.path.append('..')
from shared_utils import get_mongo_client, get_minio_client, log_execution, download_from_minio

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
REDIS_HOST = config["REDIS_HOST"]
REDIS_PORT = config["REDIS_PORT"]

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]
s3_client = get_minio_client(config)
bucket = config["MINIO_BUCKET"]

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
    transcript_uri = job.get("artifacts", {}).get("transcript")
    
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
    key = f"job:{job_id}"
    
    try:
        job = json.loads(r.get(key))
        
        job["stages"]["scoring"] = "running"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        ranked_segments = score_segments(job)

        if "scores" not in job:
            job["scores"] = {}
        job["scores"]["segments"] = ranked_segments[:50]
        job["stages"]["scoring"] = "done"
        job["status"] = "completed"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

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
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "scoring", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        job = json.loads(r.get(key))
        job["stages"]["scoring"] = "failed"
        job["status"] = "failed"
        job["error"] = str(e)
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))
        
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

print("Scoring worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
print(f"Listening on {LISTEN_QUEUE}")
ch.start_consuming()
