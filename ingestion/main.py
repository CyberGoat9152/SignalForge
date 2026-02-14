import streamlit as st
import redis
import pika
import uuid
import json
import time
import yaml
from datetime import datetime, timezone
import sys
sys.path.append('..')
from shared_utils import get_mongo_client, log_execution

config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
QUEUE = config["QUEUE_NAME"]
REDIS_HOST = config["REDIS_HOST"]
REDIS_PORT = config["REDIS_PORT"]

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]

def publish_job(msg):
    conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_publish(exchange="", routing_key=QUEUE, body=json.dumps(msg))
    conn.close()

st.title("SignalForge - Multimodal Pipeline")

st.header("Submit Job")

url = st.text_input("YouTube URL or Video Path")

if st.button("Submit") and url:
    job_id = str(uuid.uuid4())
    now = datetime.now(tz=timezone.utc).isoformat()

    job_state = {
        "job_id": job_id,
        "url": url,
        "status": "submitted",
        "stages": {
            "download": "pending",
            "split": "pending",
            "transcribe": "pending",
            "audio_features": "pending",
            "metadata_public": "pending",
            "scoring": "pending"
        },
        "artifacts": {},
        "features": {},
        "metadata": {},
        "scores": {},
        "created_at": now,
        "updated_at": now
    }

    r.set(f"job:{job_id}", json.dumps(job_state))

    log_execution(mongo_db, "ingestion_logs", {
        "job_id": job_id,
        "url": url,
        "action": "job_submitted",
        "status": "success"
    })

    publish_job({
        "job_id": job_id,
        "url": url,
        "stage": "download"
    })

    st.session_state["job_id"] = job_id
    st.success(f"Job submitted: {job_id}")

st.header("Job Status")

job_id_input = st.text_input("Job ID", value=st.session_state.get("job_id", ""))
if st.button("Refresh") and job_id_input:
    raw = r.get(f"job:{job_id_input}")
    if raw:
        job_data = json.loads(raw)
        st.json(job_data)
        
        st.subheader("Execution Logs")
        logs = list(mongo_db["ingestion_logs"].find({"job_id": job_id_input}, {"_id": 0}).sort("logged_at", -1))
        if logs:
            st.json(logs)
    else:
        st.error("Job not found")
