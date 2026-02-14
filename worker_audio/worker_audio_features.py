import logging
import sys
import os
import json
import time
import yaml
import pika
import redis
import librosa
import numpy as np
from datetime import datetime, timezone

from shared_utils import (
    get_mongo_client,
    get_minio_client,
    log_execution,
    download_from_minio
)

sys.path.append("..")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("audio_features_worker")

# ---------------- Config ---------------- #

logger.info("Loading config...")
config = yaml.safe_load(open("./config.yaml", "r"))

RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]
REDIS_HOST = config["REDIS_HOST"]
REDIS_PORT = config["REDIS_PORT"]
bucket = config["MINIO_BUCKET"]

# ---------------- Clients ---------------- #

logger.info("Connecting to Redis...")
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

logger.info("Connecting to Mongo...")
mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]

logger.info("Connecting to MinIO...")
s3_client = get_minio_client(config)

# ---------------- Core Logic ---------------- #

def update_job(job_id, job):
    key = f"job:{job_id}"
    job["updated_at"] = datetime.now(timezone.utc).isoformat()
    r.set(key, json.dumps(job))


def extract_features(local_audio, sr=16000, window_seconds=2):
    logger.info(f"Loading audio: {local_audio}")
    y, sr = librosa.load(local_audio, sr=sr)
    logger.info(f"Audio loaded: samples={len(y)}, sr={sr}")

    hop = int(sr * window_seconds)
    features = []

    for i in range(0, len(y), hop):
        chunk = y[i:i+hop]
        if len(chunk) == 0:
            continue

        rms = float(np.mean(librosa.feature.rms(y=chunk)))
        zcr = float(np.mean(librosa.feature.zero_crossing_rate(chunk)))

        pitches, _ = librosa.piptrack(y=chunk, sr=sr)
        pitch = float(np.mean(pitches[pitches > 0])) if np.any(pitches > 0) else 0.0

        features.append({
            "start": float(i / sr),
            "end": float((i + hop) / sr),
            "rms": rms,
            "zcr": zcr,
            "pitch": pitch
        })

    logger.info(f"Extracted {len(features)} windows")
    return features, len(y), sr


def callback(ch, method, properties, body):
    start_time = time.time()
    raw = body.decode()
    logger.info(f"Received message: {raw}")

    msg = json.loads(raw)
    if msg.get("stage") != "audio_features":
        logger.warning("Wrong stage, requeueing")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    key = f"job:{job_id}"
    local_audio = f"/tmp/{job_id}_audio.wav"

    try:
        job_raw = r.get(key)
        if not job_raw:
            raise RuntimeError("Job not found in Redis")

        job = json.loads(job_raw)
        audio_uri = job["artifacts"]["audio"]

        logger.info(f"[{job_id}] Stage started")
        job["stages"]["audio_features"] = "running"
        update_job(job_id, job)

        s3_key = audio_uri.replace(f"s3://{bucket}/", "")
        logger.info(f"[{job_id}] Downloading {s3_key}")
        download_from_minio(s3_client, bucket, s3_key, local_audio)

        features, total_samples, sr = extract_features(local_audio)

        job.setdefault("features", {})
        job["features"]["audio"] = features
        job["stages"]["audio_features"] = "done"
        update_job(job_id, job)

        execution_time = time.time() - start_time

        log_execution(mongo_db, "audio_features", {
            "job_id": job_id,
            "feature_count": len(features),
            "duration_seconds": float(total_samples / sr),
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        logger.info(f"[{job_id}] Completed in {execution_time:.2f}s")

        os.remove(local_audio)

        ch.basic_publish(
            exchange="",
            routing_key=DONE_QUEUE,
            body=json.dumps({"job_id": job_id, "stage": "scoring"})
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        execution_time = time.time() - start_time
        logger.exception(f"[{job_id}] Failed")

        log_execution(mongo_db, "audio_features", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })

        try:
            job = json.loads(r.get(key))
            job["stages"]["audio_features"] = "failed"
            job["error"] = str(e)
            update_job(job_id, job)
        except Exception:
            logger.error("Could not update job state in Redis")

        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# ---------------- Bootstrap ---------------- #

logger.info("Audio features worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()

ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.queue_declare(queue=DONE_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)

ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
logger.info(f"Listening on queue: {LISTEN_QUEUE}")

ch.start_consuming()
