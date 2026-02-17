import pika
import json
import yaml
import time
import os
from datetime import datetime, timezone
import sys
import logging
from faster_whisper import WhisperModel
from shared_utils import (
    get_mongo_client, get_minio_client, log_execution, download_from_minio, upload_to_minio,
    get_job, update_job_stage, update_job_artifacts, set_job_error
)

sys.path.append('..')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("trascription_worker")

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]
s3_client = get_minio_client(config)
bucket = config["MINIO_BUCKET"]

# Initialize the model once
model = WhisperModel("medium", device="cpu", compute_type="int8")

def callback(ch, method, properties, body):
    start_time = time.time()
    msg = json.loads(body)
    
    if msg.get("stage") != "transcribe":
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    
    try:
        job = get_job(mongo_db, job_id)
        if not job:
            raise Exception(f"Job {job_id} not found in MongoDB")
            
        audio_uri = job["artifacts"]["audio"]
        
        update_job_stage(mongo_db, job_id, "transcribe", "running")

        s3_key = audio_uri.replace(f"s3://{bucket}/", "")
        local_audio = f"/tmp/{job_id}_audio.wav"
        download_from_minio(s3_client, bucket, s3_key, local_audio)

        segments, info = model.transcribe(
            local_audio,
            language="en",
            beam_size=5,
            vad_filter=True
        )

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

        output_dir = f"/tmp/{job_id}_transcript"
        os.makedirs(output_dir, exist_ok=True)
        transcript_file = os.path.join(output_dir, "transcript.json")
        
        with open(transcript_file, 'w') as tf:
            json.dump(transcript_data, tf, indent=2)

        transcript_s3_key = f"{job_id}/transcript.json"
        transcript_uri = upload_to_minio(s3_client, bucket, transcript_s3_key, transcript_file)

        update_job_stage(mongo_db, job_id, "transcribe", "done")
        update_job_artifacts(mongo_db, job_id, "transcript", transcript_uri)

        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "transcriptions", {
            "job_id": job_id,
            "transcript_uri": transcript_uri,
            "word_count": word_count,
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        os.remove(local_audio)
        if os.path.exists(transcript_file):
            os.remove(transcript_file)
        if os.path.exists(output_dir):
            os.rmdir(output_dir)

        next_msg = {"job_id": job_id, "stage": "metadata_public"}
        ch.basic_publish(exchange="", routing_key=DONE_QUEUE, body=json.dumps(next_msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        logger.info(f"[Worker-Transcription] [{job_id}] <OK> Transcribe completed for job ")
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"[Worker-Transcription] [{job_id}] <Failed> Transcribe failed for job see mongodb for logging")
        
        log_execution(mongo_db, "transcriptions", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        set_job_error(mongo_db, job_id, "transcribe", str(e))
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

logger.info("[Worker-Transcription] Transcribe worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.queue_declare(queue=DONE_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
logger.info("[Worker-Transcription] Transcribe worker starting...OK")
logger.info(f"[Worker-Transcription] Listening on {LISTEN_QUEUE}")
ch.start_consuming()
