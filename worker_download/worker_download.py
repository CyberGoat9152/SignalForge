import pika
import json
import subprocess
import os
import yaml
import time
from datetime import datetime, timezone
import logging
import sys
from shared_utils import (
    get_mongo_client, get_minio_client, log_execution, upload_to_minio,
    get_job, update_job_stage, update_job_artifacts, set_job_error
)

sys.path.append('..')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("download_worker")

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]

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
    
    try:
        job = get_job(mongo_db, job_id)
        if not job:
            raise Exception(f"[Worker-Download] Job {job_id} not found in MongoDB")
        
        update_job_stage(mongo_db, job_id, "download", "running")

        local_path = f"/tmp/{job_id}_raw.mp4"
        
        subprocess.run([
            "yt-dlp", "-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "-o", local_path, url
        ], check=True, timeout=600)

        file_size = os.path.getsize(local_path)
        s3_key = f"{job_id}/raw.mp4"
        s3_uri = upload_to_minio(s3_client, bucket, s3_key, local_path)

        update_job_stage(mongo_db, job_id, "download", "done")
        update_job_artifacts(mongo_db, job_id, "raw_video", s3_uri)

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
        
        logger.info(f"[Worker-Download][{job_id}] <ok> Download completed for job ")
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        logger.error(f"[Worker-Download][{job_id}] <Failed> Download failed for job see mongodb for logging")
        
        log_execution(mongo_db, "downloads", {
            "job_id": job_id,
            "url": url,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        set_job_error(mongo_db, job_id, "download", str(e))
        
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

logger.info("[Worker-Download] Download worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
logger.info("[Worker-Download] Download worker starting...OK")
logger.info("[Worker-Download] Listening on {LISTEN_QUEUE}")
ch.start_consuming()
