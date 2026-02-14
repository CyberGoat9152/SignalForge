import pika
import json
import yaml
import redis
import subprocess
import time
from datetime import datetime, timezone
import sys
sys.path.append('..')
from shared_utils import get_mongo_client, log_execution

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]
REDIS_HOST = config["REDIS_HOST"]
REDIS_PORT = config["REDIS_PORT"]

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]

def callback(ch, method, properties, body):
    start_time = time.time()
    msg = json.loads(body)
    
    if msg.get("stage") != "metadata_public":
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    key = f"job:{job_id}"
    
    try:
        job = json.loads(r.get(key))
        
        job["stages"]["metadata_public"] = "running"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        metadata = {}
        url = job.get("url")
        
        if url and ("youtube.com" in url or "youtu.be" in url):
            result = subprocess.run([
                "yt-dlp", "--dump-json", "--no-download", url
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                video_info = json.loads(result.stdout)
                
                metadata = {
                    "video_id": video_info.get("id"),
                    "title": video_info.get("title"),
                    "uploader": video_info.get("uploader"),
                    "duration": video_info.get("duration"),
                    "view_count": video_info.get("view_count", 0),
                    "like_count": video_info.get("like_count", 0),
                    "comment_count": video_info.get("comment_count", 0),
                    "upload_date": video_info.get("upload_date"),
                    "description": video_info.get("description", "")[:500],
                    "tags": video_info.get("tags", [])[:10]
                }
                
                if metadata["view_count"] and metadata["duration"]:
                    upload_date = metadata.get("upload_date")
                    if upload_date:
                        try:
                            upload_dt = datetime.strptime(upload_date, "%Y%m%d")
                            days_since = (datetime.now() - upload_dt).days
                            if days_since > 0:
                                metadata["views_per_day"] = metadata["view_count"] / days_since
                            else:
                                metadata["views_per_day"] = metadata["view_count"]
                        except:
                            metadata["views_per_day"] = 0
                
                if metadata["view_count"] > 0 and metadata["like_count"]:
                    metadata["like_ratio"] = metadata["like_count"] / metadata["view_count"]
                else:
                    metadata["like_ratio"] = 0
                
                engagement = 0
                if metadata["view_count"] > 0:
                    engagement = (metadata["like_count"] + metadata["comment_count"] * 2) / metadata["view_count"]
                metadata["engagement_score"] = engagement
        
        if "metadata" not in job:
            job["metadata"] = {}
        job["metadata"]["public"] = metadata
        job["stages"]["metadata_public"] = "done"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "metadata_public", {
            "job_id": job_id,
            "video_id": metadata.get("video_id"),
            "view_count": metadata.get("view_count", 0),
            "engagement_score": metadata.get("engagement_score", 0),
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        ch.basic_publish(exchange="", routing_key=DONE_QUEUE, body=json.dumps({
            "job_id": job_id,
            "stage": "scoring"
        }))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "metadata_public", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        job = json.loads(r.get(key))
        job["stages"]["metadata_public"] = "done"
        job["metadata"] = {"public": {}}
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))
        
        ch.basic_publish(exchange="", routing_key=DONE_QUEUE, body=json.dumps({
            "job_id": job_id,
            "stage": "scoring"
        }))
        ch.basic_ack(delivery_tag=method.delivery_tag)

print("Metadata public worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.queue_declare(queue=DONE_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
print(f"Listening on {LISTEN_QUEUE}")
ch.start_consuming()
