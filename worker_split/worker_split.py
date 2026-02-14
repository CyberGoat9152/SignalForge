import pika
import json
import redis
import subprocess
import yaml
import os
import time
from datetime import datetime, timezone
import sys
import logging
import re

sys.path.append('..')
from shared_utils import get_mongo_client, get_minio_client, log_execution, upload_to_minio, download_from_minio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]
REDIS_HOST = config["REDIS_HOST"]
REDIS_PORT = config["REDIS_PORT"]

# FFmpeg configuration
MAX_FFMPEG_TIMEOUT = 3600  # 1 hour hard limit
MIN_FFMPEG_TIMEOUT = 600   # 10 minutes minimum

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]
s3_client = get_minio_client(config)
bucket = config["MINIO_BUCKET"]


def get_video_duration(video_path):
    """Get video duration in seconds using ffprobe"""
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path
        ], capture_output=True, text=True, timeout=30)
        
        duration = float(result.stdout.strip())
        return duration
    except Exception as e:
        logger.warning(f"Could not get video duration: {e}")
        return None


def calculate_timeout(file_path):
    """Calculate appropriate timeout based on file size and duration"""
    try:
        # Get file size
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        # Get video duration
        duration = get_video_duration(file_path)
        
        if duration:
            # Base timeout on duration: roughly 2x realtime for encoding
            # (e.g., 10 min video = 20 min timeout)
            timeout = int(duration * 2)
            logger.info(f"Video duration: {duration:.1f}s, calculated timeout: {timeout}s")
        else:
            # Fallback: estimate based on file size (1 minute per 50MB)
            timeout = int(size_mb * 1.2)
            logger.info(f"Video size: {size_mb:.1f}MB, estimated timeout: {timeout}s")
        
        # Apply bounds
        timeout = max(MIN_FFMPEG_TIMEOUT, min(timeout, MAX_FFMPEG_TIMEOUT))
        return timeout
        
    except Exception as e:
        logger.warning(f"Error calculating timeout: {e}, using default")
        return MIN_FFMPEG_TIMEOUT


def run_ffmpeg_with_progress(cmd, timeout, job_id, operation):
    """Run FFmpeg command with progress logging"""
    logger.info(f"Running FFmpeg {operation} for job {job_id}: {' '.join(cmd)}")
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        start_time = time.time()
        last_log_time = start_time
        
        # Read stderr for progress
        for line in process.stderr:
            current_time = time.time()
            
            # Log progress every 30 seconds
            if current_time - last_log_time > 30:
                # Parse progress if available (time=00:01:23.45)
                time_match = re.search(r'time=(\d+):(\d+):(\d+\.\d+)', line)
                if time_match:
                    h, m, s = time_match.groups()
                    progress_seconds = int(h) * 3600 + int(m) * 60 + float(s)
                    elapsed = current_time - start_time
                    logger.info(f"Job {job_id} {operation}: processed {progress_seconds:.1f}s of video in {elapsed:.1f}s")
                last_log_time = current_time
            
            # Check timeout
            if current_time - start_time > timeout:
                process.kill()
                raise subprocess.TimeoutExpired(cmd, timeout)
        
        # Wait for process to complete
        return_code = process.wait(timeout=10)
        
        if return_code != 0:
            stdout, stderr = process.communicate()
            raise subprocess.CalledProcessError(return_code, cmd, stderr=stderr)
            
        elapsed = time.time() - start_time
        logger.info(f"Job {job_id} {operation} completed in {elapsed:.1f}s")
        
    except subprocess.TimeoutExpired:
        logger.error(f"Job {job_id} {operation} timed out after {timeout}s")
        try:
            process.kill()
        except:
            pass
        raise


def get_rabbitmq_connection():
    """Create RabbitMQ connection with heartbeat settings"""
    parameters = pika.URLParameters(RABBIT_URL)
    # Set heartbeat to 2x max FFmpeg timeout to be safe
    parameters.heartbeat = MAX_FFMPEG_TIMEOUT * 2
    parameters.blocked_connection_timeout = 600
    return pika.BlockingConnection(parameters)


def callback(ch, method, properties, body):
    start_time = time.time()
    msg = json.loads(body)
    
    if msg.get("stage") != "split":
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    key = f"job:{job_id}"
    
    try:
        logger.info(f"Processing job {job_id}")
        
        job = json.loads(r.get(key))
        raw_video_uri = job["artifacts"]["raw_video"]
        
        job["stages"]["split"] = "running"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        s3_key = raw_video_uri.replace(f"s3://{bucket}/", "")
        local_video = f"/tmp/{job_id}_raw.mp4"
        
        logger.info(f"Downloading video from S3 for job {job_id}")
        download_from_minio(s3_client, bucket, s3_key, local_video)

        local_audio = f"/tmp/{job_id}_audio.wav"
        local_video_only = f"/tmp/{job_id}_video.mp4"

        # Calculate timeout based on video characteristics
        ffmpeg_timeout = calculate_timeout(local_video)
        video_size_mb = os.path.getsize(local_video) / (1024 * 1024)
        logger.info(f"Processing {video_size_mb:.1f}MB video with {ffmpeg_timeout}s timeout")

        # Extract audio with progress tracking
        audio_cmd = [
            "ffmpeg", "-y", "-i", local_video,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            "-progress", "pipe:2",  # Progress to stderr
            local_audio
        ]
        run_ffmpeg_with_progress(audio_cmd, ffmpeg_timeout, job_id, "audio extraction")

        # Extract video with faster settings for large files
        preset = "ultrafast" if video_size_mb > 500 else "fast"
        
        video_cmd = [
            "ffmpeg", "-y", "-i", local_video,
            "-an", "-vcodec", "libx264", "-preset", preset,
            "-crf", "23",  # Reasonable quality
            "-progress", "pipe:2",
            local_video_only
        ]
        run_ffmpeg_with_progress(video_cmd, ffmpeg_timeout, job_id, "video extraction")

        audio_s3_key = f"{job_id}/audio.wav"
        video_s3_key = f"{job_id}/video.mp4"
        
        logger.info(f"Uploading artifacts for job {job_id}")
        audio_uri = upload_to_minio(s3_client, bucket, audio_s3_key, local_audio)
        video_uri = upload_to_minio(s3_client, bucket, video_s3_key, local_video_only)

        audio_size = os.path.getsize(local_audio)
        video_size = os.path.getsize(local_video_only)

        job["stages"]["split"] = "done"
        job["artifacts"]["audio"] = audio_uri
        job["artifacts"]["video"] = video_uri
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))

        execution_time = time.time() - start_time
        
        log_execution(mongo_db, "splits", {
            "job_id": job_id,
            "audio_uri": audio_uri,
            "video_uri": video_uri,
            "audio_size_bytes": audio_size,
            "video_size_bytes": video_size,
            "input_size_bytes": os.path.getsize(local_video),
            "execution_time_seconds": execution_time,
            "timeout_used_seconds": ffmpeg_timeout,
            "status": "success"
        })

        # Cleanup local files
        for f in [local_video, local_audio, local_video_only]:
            if os.path.exists(f):
                os.remove(f)
                logger.debug(f"Removed temp file: {f}")

        logger.info(f"Publishing downstream messages for job {job_id}")
        
        # Publish messages with error handling
        try:
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
        except Exception as pub_error:
            logger.error(f"Error publishing messages for job {job_id}: {pub_error}")
            raise
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Successfully completed job {job_id} in {execution_time:.2f}s")
        
    except subprocess.TimeoutExpired as e:
        execution_time = time.time() - start_time
        error_msg = f"FFmpeg timeout after {e.timeout}s - video may be too large or corrupted"
        logger.error(f"Timeout processing job {job_id}: {error_msg}")
        
        try:
            log_execution(mongo_db, "splits", {
                "job_id": job_id,
                "error": error_msg,
                "execution_time_seconds": execution_time,
                "timeout_seconds": e.timeout,
                "status": "failed"
            })
            
            job = json.loads(r.get(key))
            job["stages"]["split"] = "failed"
            job["status"] = "failed"
            job["error"] = error_msg
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            r.set(key, json.dumps(job))
        except Exception as log_error:
            logger.error(f"Error logging timeout for job {job_id}: {log_error}")
        
        # Cleanup
        for f in [f"/tmp/{job_id}_raw.mp4", f"/tmp/{job_id}_audio.wav", f"/tmp/{job_id}_video.mp4"]:
            try:
                if os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass
        
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except (pika.exceptions.AMQPConnectionError, 
                pika.exceptions.AMQPChannelError,
                pika.exceptions.StreamLostError) as nack_error:
            logger.error(f"Could not nack message due to connection error: {nack_error}")
            raise
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Error processing job {job_id}: {e}", exc_info=True)
        
        try:
            log_execution(mongo_db, "splits", {
                "job_id": job_id,
                "error": str(e),
                "execution_time_seconds": execution_time,
                "status": "failed"
            })
            
            job = json.loads(r.get(key))
            job["stages"]["split"] = "failed"
            job["status"] = "failed"
            job["error"] = str(e)
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            r.set(key, json.dumps(job))
        except Exception as log_error:
            logger.error(f"Error logging failure for job {job_id}: {log_error}")
        
        # Cleanup temp files on error
        for f in [f"/tmp/{job_id}_raw.mp4", f"/tmp/{job_id}_audio.wav", f"/tmp/{job_id}_video.mp4"]:
            try:
                if os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass
        
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except (pika.exceptions.AMQPConnectionError, 
                pika.exceptions.AMQPChannelError,
                pika.exceptions.StreamLostError) as nack_error:
            logger.error(f"Could not nack message due to connection error: {nack_error}")
            raise


def main():
    """Main worker loop with auto-reconnect"""
    retry_delay = 5
    max_retry_delay = 60
    
    while True:
        try:
            logger.info("Split worker starting...")
            conn = get_rabbitmq_connection()
            ch = conn.channel()
            
            # Declare queues
            ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
            ch.queue_declare(queue=DONE_QUEUE, durable=True)
            ch.queue_declare(queue="q.audio_features", durable=True)
            
            # Set QoS to process one message at a time
            ch.basic_qos(prefetch_count=1)
            
            # Start consuming
            ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
            logger.info(f"Listening on {LISTEN_QUEUE} (max FFmpeg timeout: {MAX_FFMPEG_TIMEOUT}s)")
            
            # Reset retry delay on successful connection
            retry_delay = 5
            
            ch.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            try:
                ch.stop_consuming()
                conn.close()
            except Exception:
                pass
            break
            
        except (pika.exceptions.AMQPConnectionError,
                pika.exceptions.StreamLostError,
                pika.exceptions.ChannelClosedByBroker) as e:
            logger.error(f"Connection lost: {e}. Reconnecting in {retry_delay}s...")
            try:
                conn.close()
            except Exception:
                pass
            
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            try:
                conn.close()
            except Exception:
                pass
            
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)


if __name__ == "__main__":
    main()