import pika
import json
import yaml
import redis
import subprocess
import time
import os
from datetime import datetime, timezone
import sys
sys.path.append('..')
from shared_utils import get_mongo_client, get_minio_client, log_execution, download_from_minio, upload_to_minio

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
    
    if msg.get("stage") != "transcribe":
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    key = f"job:{job_id}"
    
    try:
        job = json.loads(r.get(key))
        audio_uri = job["artifacts"]["audio"]
        
        job["stages"]["transcribe"] = "running"
        job["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        s3_key = audio_uri.replace(f"s3://{bucket}/", "")
        local_audio = f"/tmp/{job_id}_audio.wav"
        download_from_minio(s3_client, bucket, s3_key, local_audio)

        output_dir = f"/tmp/{job_id}_transcript"
        os.makedirs(output_dir, exist_ok=True)

        subprocess.run([
            "whisper", local_audio,
            "--model", "base",
            "--language", "en",
            "--output_format", "json",
            "--output_dir", output_dir
        ], check=True, timeout=1800)

        transcript_file = None
        for f in os.listdir(output_dir):
            if f.endswith(".json"):
                transcript_file = os.path.join(output_dir, f)
                break

        if transcript_file:
            transcript_s3_key = f"{job_id}/transcript.json"
            transcript_uri = upload_to_minio(s3_client, bucket, transcript_s3_key, transcript_file)
            
            with open(transcript_file, 'r') as tf:
                transcript_data = json.load(tf)
            
            word_count = len(transcript_data.get("text", "").split())
        else:
            transcript_uri = None
            word_count = 0

        job["stages"]["transcribe"] = "done"
        job["artifacts"]["transcript"] = transcript_uri
        job["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "transcriptions", {
            "job_id": job_id,
            "transcript_uri": transcript_uri,
            "word_count": word_count,
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        os.remove(local_audio)
        if transcript_file and os.path.exists(transcript_file):
            os.remove(transcript_file)
        if os.path.exists(output_dir):
            os.rmdir(output_dir)

        next_msg = {
            "job_id": job_id,
            "stage": "metadata_public"
        }
        ch.basic_publish(exchange="", routing_key=DONE_QUEUE, body=json.dumps(next_msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "transcriptions", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        job = json.loads(r.get(key))
        job["stages"]["transcribe"] = "failed"
        job["status"] = "failed"
        job["error"] = str(e)
        job["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        r.set(key, json.dumps(job))
        
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

print("Transcribe worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.queue_declare(queue=DONE_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
print(f"Listening on {LISTEN_QUEUE}")
ch.start_consuming()
