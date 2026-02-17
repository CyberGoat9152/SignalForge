import pika
import json
import subprocess
import yaml
import time
from datetime import datetime, timezone
import sys
import logging
import os
from shared_utils import (
    get_mongo_client, get_minio_client,
    log_execution, upload_to_minio,
    get_job, update_job_stage,
    update_job_artifacts, set_job_error
)

sys.path.append('..')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("split_worker")

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]
s3_client = get_minio_client(config)
bucket = config["MINIO_BUCKET"]

def run_streaming_ffmpeg(input_stream, audio_out, video_out, job_id):
    cmd = [
        "ffmpeg", "-y",
        "-i", "pipe:0",
        "-map", "0:a", "-vn",
        "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", audio_out,
        "-map", "0:v", "-an",
        "-c:v", "copy", video_out
    ]

    logger.info(f"Job {job_id}: starting single-pass streaming FFmpeg")

    p = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    start = time.time()
    bytes_streamed = 0

    try:
        for chunk in input_stream.iter_chunks(chunk_size=1024 * 1024):
            if not chunk:
                break
            p.stdin.write(chunk)
            bytes_streamed += len(chunk)

        p.stdin.close()
        p.wait()

        if p.returncode != 0:
            stderr = p.stderr.read().decode()
            raise RuntimeError(stderr)

        elapsed = time.time() - start
        logger.info(
            f"Job {job_id}: streamed {bytes_streamed/1024/1024:.1f}MB "
            f"in {elapsed:.1f}s"
        )

    finally:
        try:
            p.kill()
        except:
            pass

def callback(ch, method, properties, body):
    start_time = time.time()
    msg = json.loads(body)

    if msg.get("stage") != "split":
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]

    try:
        logger.info(f"Processing job {job_id}")

        job = get_job(mongo_db, job_id)
        if not job:
            raise Exception(f"Job {job_id} not found")

        raw_video_uri = job["artifacts"]["raw_video"]
        s3_key = raw_video_uri.replace(f"s3://{bucket}/", "")

        update_job_stage(mongo_db, job_id, "split", "running")

        audio_path = f"/tmp/{job_id}_audio.wav"
        video_path = f"/tmp/{job_id}_video.mp4"

        logger.info(f"Job {job_id}: streaming from MinIO")

        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
        stream = obj["Body"]

        run_streaming_ffmpeg(stream, audio_path, video_path, job_id)

        audio_s3_key = f"{job_id}/audio.wav"
        video_s3_key = f"{job_id}/video.mp4"

        audio_uri = upload_to_minio(s3_client, bucket, audio_s3_key, audio_path)
        video_uri = upload_to_minio(s3_client, bucket, video_s3_key, video_path)

        update_job_stage(mongo_db, job_id, "split", "done")
        update_job_artifacts(mongo_db, job_id, "audio", audio_uri)
        update_job_artifacts(mongo_db, job_id, "video", video_uri)

        execution_time = time.time() - start_time

        log_execution(mongo_db, "splits", {
            "job_id": job_id,
            "audio_uri": audio_uri,
            "video_uri": video_uri,
            "execution_time_seconds": execution_time,
            "status": "success"
        })

        for f in [audio_path, video_path]:
            try:
                os.remove(f)
            except:
                pass

        ch.basic_publish(
            exchange="",
            routing_key=DONE_QUEUE,
            body=json.dumps({"job_id": job_id, "stage": "transcribe"}),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        ch.basic_publish(
            exchange="",
            routing_key="q.audio_features",
            body=json.dumps({"job_id": job_id, "stage": "audio_features"}),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(
            f"[Worker-Split] [{job_id}] <OK> Job finished in {execution_time:.2f}s"
        )

    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"[Worker-Split] [{job_id}] <Failed> Job failed see mongodb for logging", exc_info=True)

        log_execution(mongo_db, "splits", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })

        set_job_error(mongo_db, job_id, "split", str(e))

        for f in [f"/tmp/{job_id}_audio.wav", f"/tmp/{job_id}_video.mp4"]:
            try:
                os.remove(f)
            except:
                pass

        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

logger.info("[Worker-Split] Streaming split worker starting...")
conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
ch = conn.channel()
ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
ch.queue_declare(queue=DONE_QUEUE, durable=True)
ch.queue_declare(queue="q.audio_features", durable=True)
ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
logger.info("[Worker-Split] Streaming split worker starting...")
logger.info(f"[Worker-Split] Listening on {LISTEN_QUEUE}")
ch.start_consuming()
