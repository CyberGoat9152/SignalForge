import logging
import sys
import os
import json
import time
import yaml
import pika
import librosa
import numpy as np
from datetime import datetime, timezone

from shared_utils import (
    get_mongo_client,
    get_minio_client,
    log_execution,
    download_from_minio_direct,
    get_job,
    update_job_stage,
    update_job_features,
    set_job_error
)

sys.path.append("..")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("audio_features_worker")

logger.info("[Worker-Audio-Features] Loading config...")
config = yaml.safe_load(open("./config.yaml", "r"))

RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]
bucket = config["MINIO_BUCKET"]

# Clients
logger.info("[Worker-Audio-Features] Connecting to Mongo...")
mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]

logger.info("[Worker-Audio-Features] Connecting to MinIO...")
s3_client = get_minio_client(config)


def extract_features(local_audio, sr=16000, window_seconds=2):
    logger.info(f"Loading audio: {local_audio}")
    
    if not os.path.exists(local_audio):
        raise FileNotFoundError(f"Audio file not found: {local_audio}")
    
    file_size = os.path.getsize(local_audio)
    if file_size == 0:
        raise ValueError(f"Audio file is empty (0 bytes): {local_audio}")
    
    logger.info(f"[Worker-Audio-Features] Audio file found: {local_audio} ({file_size} bytes)")
    
    y, sr = librosa.load(local_audio, sr=sr)
    logger.info(f"[Worker-Audio-Features] Audio loaded: samples={len(y)}, sr={sr}")

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

    logger.info(f"[Worker-Audio-Features] Extracted {len(features)} windows")
    return features, len(y), sr


def callback(ch, method, properties, body):
    start_time = time.time()
    raw = body.decode()
    logger.info(f"[Worker-Audio-Features] Received message: {raw}")

    msg = json.loads(raw)
    if msg.get("stage") != "audio_features":
        logger.warning("[Worker-Audio-Features] Wrong stage, requeueing")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    local_audio = f"/tmp/{job_id}_audio.wav"

    try:
        job = get_job(mongo_db, job_id)
        if not job:
            raise RuntimeError(f"[Worker-Audio-Features] Job {job_id} not found in MongoDB")

        audio_uri = job["artifacts"]["audio"]

        logger.info(f"[{job_id}] Stage started")
        update_job_stage(mongo_db, job_id, "audio_features", "running")

        s3_key = audio_uri.replace(f"s3://{bucket}/", "")
        logger.info(f"[Worker-Audio-Features] {job_id}] Downloading from MinIO - Bucket: {bucket}, Key: {s3_key}")
        
        try:
            download_from_minio_direct(s3_client, bucket, s3_key, local_audio)
        except Exception as download_error:
            logger.error(f"[Worker-Audio-Features] [{job_id}] MinIO download failed: {download_error}")
            raise RuntimeError(f"[Worker-Audio-Features] Failed to download audio from MinIO: {download_error}")
        
        if not os.path.exists(local_audio):
            raise FileNotFoundError(f"[Worker-Audio-Features] Download completed but file not found at {local_audio}")
        
        file_size = os.path.getsize(local_audio)
        if file_size == 0:
            raise ValueError(f"[Worker-Audio-Features] Downloaded audio file is empty (0 bytes)")
        
        logger.info(f"[Worker-Audio-Features] [{job_id}] Download successful: {file_size} bytes")

        features, total_samples, sr = extract_features(local_audio)

        update_job_features(mongo_db, job_id, "audio", features)
        update_job_stage(mongo_db, job_id, "audio_features", "done")

        execution_time = time.time() - start_time

        log_execution(mongo_db, "audio_features", {
            "job_id": job_id,
            "feature_count": len(features),
            "duration_seconds": float(total_samples / sr),
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        logger.info(f"[Worker-Audio-Features][{job_id}] <OK> Completed in {execution_time:.2f}s")

        try:
            os.remove(local_audio)
        except Exception as cleanup_error:
            logger.warning(f"[Worker-Audio-Features] [{job_id}] <Failed> to clean up {local_audio}: {cleanup_error}")

        # Send to next stage
        ch.basic_publish(
            exchange="",
            routing_key=DONE_QUEUE,
            body=json.dumps({"job_id": job_id, "stage": "scoring"})
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        execution_time = time.time() - start_time
        logger.exception(f"[Worker-Audio-Features] [{job_id}]  <Failed> see mongodb for error logging")

        log_execution(mongo_db, "audio_features", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })

        set_job_error(mongo_db, job_id, "audio_features", str(e))

        try:
            if os.path.exists(local_audio):
                os.remove(local_audio)
        except Exception:
            pass

        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


logger.info("[Worker-Audio-Features] Audio features worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()

ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.queue_declare(queue=DONE_QUEUE, durable=True)
ch.basic_qos(prefetch_count=1)

ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
logger.info(f"[Worker-Audio-Features] Audio features worker starting... <OK>")
logger.info(f"[Worker-Audio-Features] Listening on queue: {LISTEN_QUEUE}")

ch.start_consuming()