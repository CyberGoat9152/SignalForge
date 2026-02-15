import pika
import json
import yaml
import redis
import subprocess
import time
from datetime import datetime, timezone
import sys
import logging

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("=" * 60)
logger.info("METADATA PUBLIC WORKER INITIALIZING")
logger.info("=" * 60)

sys.path.append('..')
from shared_utils import get_mongo_client, log_execution

# Load config
logger.info("Loading configuration...")
config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
LISTEN_QUEUE = config["LISTEN_QUEUE"]
DONE_QUEUE = config["DONE_QUEUE"]
REDIS_HOST = config["REDIS_HOST"]
REDIS_PORT = config["REDIS_PORT"]
logger.info(f"  Listen Queue: {LISTEN_QUEUE}")
logger.info(f"  Done Queue: {DONE_QUEUE}")

# Connect to Redis
logger.info("Connecting to Redis...")
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)
try:
    r.ping()
    logger.info("  ✓ Redis connection successful")
except Exception as e:
    logger.error(f"  ✗ Redis connection failed: {e}")
    raise

# Connect to MongoDB
logger.info("Connecting to MongoDB...")
try:
    mongo_client = get_mongo_client(config)
    mongo_db = mongo_client[config["MONGO_DB"]]
    logger.info("  ✓ MongoDB connection successful")
except Exception as e:
    logger.error(f"  ✗ MongoDB connection failed: {e}")
    raise

# Verify yt-dlp is available
logger.info("Checking for yt-dlp...")
try:
    result = subprocess.run(["yt-dlp", "--version"], capture_output=True, text=True, timeout=5)
    if result.returncode == 0:
        version = result.stdout.strip()
        logger.info(f"  ✓ yt-dlp available: {version}")
    else:
        logger.warning("  ⚠ yt-dlp found but version check failed")
except FileNotFoundError:
    logger.error("  ✗ yt-dlp not found! Install with: pip install yt-dlp")
    raise
except Exception as e:
    logger.warning(f"  ⚠ Could not verify yt-dlp: {e}")

def callback(ch, method, properties, body):
    start_time = time.time()
    msg = json.loads(body)
    
    if msg.get("stage") != "metadata_public":
        logger.warning(f"Received message for wrong stage: {msg.get('stage')}, requeuing")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    job_id = msg["job_id"]
    logger.info(f"[{job_id}] Processing metadata job")
    key = f"job:{job_id}"
    
    try:
        job = json.loads(r.get(key))
        
        job["stages"]["metadata_public"] = "running"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))
        logger.info(f"[{job_id}] Job status updated to 'running'")

        metadata = {}
        url = job.get("url")
        
        if url and ("youtube.com" in url or "youtu.be" in url):
            logger.info(f"[{job_id}] Fetching YouTube metadata for: {url}")
            
            try:
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
                    
                    logger.info(f"[{job_id}] Video: {metadata.get('title', 'Unknown')}")
                    logger.info(f"[{job_id}] Views: {metadata.get('view_count', 0):,}")
                    logger.info(f"[{job_id}] Likes: {metadata.get('like_count', 0):,}")
                    
                    # Calculate derived metrics
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
                                logger.info(f"[{job_id}] Views/day: {metadata['views_per_day']:.1f}")
                            except Exception as e:
                                logger.warning(f"[{job_id}] Could not calculate views_per_day: {e}")
                                metadata["views_per_day"] = 0
                    
                    if metadata["view_count"] > 0 and metadata["like_count"]:
                        metadata["like_ratio"] = metadata["like_count"] / metadata["view_count"]
                        logger.info(f"[{job_id}] Like ratio: {metadata['like_ratio']:.4f}")
                    else:
                        metadata["like_ratio"] = 0
                    
                    engagement = 0
                    if metadata["view_count"] > 0:
                        engagement = (metadata["like_count"] + metadata["comment_count"] * 2) / metadata["view_count"]
                    metadata["engagement_score"] = engagement
                    logger.info(f"[{job_id}] Engagement score: {engagement:.6f}")
                    
                else:
                    logger.error(f"[{job_id}] yt-dlp failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                logger.error(f"[{job_id}] yt-dlp timed out after 30s")
            except json.JSONDecodeError as e:
                logger.error(f"[{job_id}] Failed to parse yt-dlp output: {e}")
            except Exception as e:
                logger.error(f"[{job_id}] Error fetching metadata: {e}")
        else:
            logger.info(f"[{job_id}] URL is not a YouTube link, skipping metadata")
        
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

        next_msg = {
            "job_id": job_id,
            "stage": "scoring"
        }
        logger.info(f"[{job_id}] Publishing to queue: {DONE_QUEUE}")
        ch.basic_publish(exchange="", routing_key=DONE_QUEUE, body=json.dumps(next_msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        logger.info(f"[{job_id}] ✓ Job completed successfully in {execution_time:.2f}s")
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"[{job_id}] ✗ Job failed: {e}", exc_info=True)
        
        log_execution(mongo_db, "metadata_public", {
            "job_id": job_id,
            "error": str(e),
            "execution_time_seconds": execution_time,
            "status": "failed"
        })
        
        # Don't fail the entire pipeline - just use empty metadata
        job = json.loads(r.get(key))
        job["stages"]["metadata_public"] = "done"
        job["metadata"] = {"public": {}}
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        r.set(key, json.dumps(job))
        
        next_msg = {
            "job_id": job_id,
            "stage": "scoring"
        }
        logger.info(f"[{job_id}] Continuing pipeline despite error...")
        ch.basic_publish(exchange="", routing_key=DONE_QUEUE, body=json.dumps(next_msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)

# Connect to RabbitMQ
logger.info("Connecting to RabbitMQ...")
try:
    conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    ch = conn.channel()
    ch.queue_declare(queue=LISTEN_QUEUE, durable=True)
    ch.queue_declare(queue=DONE_QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=LISTEN_QUEUE, on_message_callback=callback)
    logger.info("  ✓ RabbitMQ connection successful")
except Exception as e:
    logger.error(f"  ✗ RabbitMQ connection failed: {e}")
    raise

logger.info("")
logger.info("=" * 60)
logger.info(f"✓ METADATA PUBLIC WORKER READY - Listening on {LISTEN_QUEUE}")
logger.info("=" * 60)
logger.info("")

ch.start_consuming()