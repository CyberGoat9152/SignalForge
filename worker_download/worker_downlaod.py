import pika
import json
import redis
import subprocess
import os
import yaml
import time
from datetime import datetime, timezone
import sys
sys.path.append('..')
from shared_utils import get_mongo_client, get_minio_client, log_execution, upload_to_minio

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
s3_client = get_minio_client(config)
bucket = config["MINIO_BUCKET"]

def callback(ch, method, properties, body):
    start_time = time.time()
    msg = json.loads(body)
    
    if msg.get("stage") != "download":
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    url = msg["url"]
    key = f"job:{job_id}"
    
    try:
        job = json.loads(r.get(key))
        
        job["stages"]["download"] = "running"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        local_path = f"/tmp/{job_id}_raw.mp4"
        
        subprocess.run([
            "yt-dlp", "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "-o", local_path, url
        ], check=True, timeout=600)

        file_size = os.path.getsize(local_path)
        s3_key = f"{job_id}/raw.mp4"
        s3_uri = upload_to_minio(s3_client, bucket, s3_key, local_path)

        job["stages"]["download"] = "done"
        job["artifacts"]["raw_video"] = s3_uri
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "downloads", {
            "job_id": job_id,
            "url": url,
            "s3_uri": s3_uri,
            "file_size_bytes": file_size,
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        os.remove(local_path)

        next_msg = {
            "job_id": job_id,
            "stage": "split"
        }
        ch.basic_publish(exchange="", routing_key=DONE_QUEUE, body=json.dumps(next_msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "downloads", {
            "job_id": job_id,
            "url": url,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        job = json.loads(r.get(key))
        job["stages"]["download"] = "failed"
        job["status"] = "failed"
        job["error"] = str(e)
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))
        
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

print("Download worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
print(f"Listening on {LISTEN_QUEUE}")
ch.start_consuming()
