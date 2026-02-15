import pika
import json
import yaml
import redis
import time
import os
from datetime import datetime, timezone
import sys
from faster_whisper import WhisperModel

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

# Initialize the model once (outside callback for efficiency)
# Options: "tiny", "base", "small", "medium", "large-v2", "large-v3"
model = WhisperModel("medium", device="cpu", compute_type="int8")
# For GPU: device="cuda", compute_type="float16"

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

        # Transcribe with faster-whisper
        segments, info = model.transcribe(
            local_audio,
            language="en",
            beam_size=5,
            vad_filter=True  # Voice activity detection helps filter silence
        )

        # Build transcript in Whisper JSON format
        transcript_data = {
            "text": "",
            "segments": [],
            "language": info.language
        }
        
        full_text = []
        for segment in segments:
            transcript_data["segments"].append({
                "id": segment.id,
                "seek": segment.seek,
                "start": segment.start,
                "end": segment.end,
                "text": segment.text,
                "tokens": segment.tokens,
                "temperature": segment.temperature,
                "avg_logprob": segment.avg_logprob,
                "compression_ratio": segment.compression_ratio,
                "no_speech_prob": segment.no_speech_prob
            })
            full_text.append(segment.text)
        
        transcript_data["text"] = " ".join(full_text).strip()
        word_count = len(transcript_data["text"].split())

        # Save transcript
        output_dir = f"/tmp/{job_id}_transcript"
        os.makedirs(output_dir, exist_ok=True)
        transcript_file = os.path.join(output_dir, "transcript.json")
        
        with open(transcript_file, 'w') as tf:
            json.dump(transcript_data, tf, indent=2)

        transcript_s3_key = f"{job_id}/transcript.json"
        transcript_uri = upload_to_minio(s3_client, bucket, transcript_s3_key, transcript_file)

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

        # Cleanup
        os.remove(local_audio)
        if os.path.exists(transcript_file):
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