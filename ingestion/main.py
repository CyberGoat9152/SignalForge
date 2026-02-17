import streamlit as st
import pika
import uuid
import json
import yaml
from datetime import datetime, timezone
import pandas as pd
import time
import sys
sys.path.append('..')
from shared_utils import (
    get_mongo_client, get_minio_client, log_execution, 
    create_job, get_job, get_all_jobs, list_minio_objects
)

# Page config
st.set_page_config(
    page_title="SignalForge - Multimodal Pipeline",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern look
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #6c757d;
        margin-bottom: 2rem;
    }
    .stage-card {
        padding: 1.5rem;
        border-radius: 12px;
        margin: 0.5rem 0;
        border-left: 4px solid;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stage-pending {
        background-color: #f8f9fa;
        border-left-color: #6c757d;
    }
    .stage-running {
        background-color: #fff3cd;
        border-left-color: #ffc107;
    }
    .stage-done {
        background-color: #d4edda;
        border-left-color: #28a745;
    }
    .stage-failed {
        background-color: #f8d7da;
        border-left-color: #dc3545;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #667eea;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #6c757d;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .pipeline-flow {
        display: flex;
        align-items: center;
        justify-content: space-around;
        padding: 2rem 0;
        flex-wrap: wrap;
    }
    .pipeline-arrow {
        font-size: 2rem;
        color: #667eea;
        margin: 0 1rem;
    }
    .worker-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        min-width: 150px;
        text-align: center;
        margin: 0.5rem;
    }
    .worker-title {
        font-size: 1rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .worker-status {
        font-size: 0.8rem;
        opacity: 0.9;
    }
    .file-item {
        background: #f8f9fa;
        padding: 0.75rem;
        border-radius: 6px;
        margin: 0.25rem 0;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .progress-ring {
        width: 120px;
        height: 120px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 12px 24px;
    }
</style>
""", unsafe_allow_html=True)

# Load config
config = yaml.safe_load(open("./config.yaml", "r").read())
RABBIT_URL = config["RABBIT_URL"]
QUEUE = config["QUEUE_NAME"]

mongo_client = get_mongo_client(config)
mongo_db = mongo_client[config["MONGO_DB"]]
mongo_db.command('ping')
s3_client = get_minio_client(config)
bucket = config["MINIO_BUCKET"]

def publish_job(msg):
    """Publish job to RabbitMQ"""
    conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_publish(exchange="", routing_key=QUEUE, body=json.dumps(msg))
    conn.close()

def get_stage_status_class(status):
    """Get CSS class for stage status"""
    if status == "pending":
        return "stage-pending"
    elif status == "running":
        return "stage-running"
    elif status == "done":
        return "stage-done"
    elif status == "failed":
        return "stage-failed"
    return "stage-pending"

def get_stage_icon(status):
    """Get emoji icon for stage status"""
    if status == "pending":
        return "⏳"
    elif status == "running":
        return "🔄"
    elif status == "done":
        return "✅"
    elif status == "failed":
        return "❌"
    return "⏳"

def render_pipeline_visualization(job_data):
    """Render the pipeline as a flow diagram"""
    stages_order = ["download", "split", "transcribe", "audio_features", "metadata_public", "scoring"]
    stages_display = {
        "download": "📥 Download",
        "split": "✂️ Split",
        "transcribe": "📝 Transcribe",
        "audio_features": "🎵 Audio",
        "metadata_public": "📊 Metadata",
        "scoring": "⭐ Scoring"
    }
    
    st.markdown("### 🔄 Pipeline Flow")
    
    # Create columns for pipeline stages
    cols = st.columns(len(stages_order))
    
    for idx, stage in enumerate(stages_order):
        with cols[idx]:
            status = job_data["stages"].get(stage, "pending")
            status_class = get_stage_status_class(status)
            icon = get_stage_icon(status)
            
            # Create worker card
            st.markdown(f"""
            <div class="worker-card" style="background: {'linear-gradient(135deg, #28a745 0%, #20c997 100%)' if status == 'done' else 'linear-gradient(135deg, #ffc107 0%, #ff9800 100%)' if status == 'running' else 'linear-gradient(135deg, #6c757d 0%, #495057 100%)' if status == 'pending' else 'linear-gradient(135deg, #dc3545 0%, #c82333 100%)'};">
                <div class="worker-title">{stages_display[stage]}</div>
                <div class="worker-status">{icon} {status.upper()}</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Add arrow except for last stage
            if idx < len(stages_order) - 1:
                st.markdown("<div style='text-align: center; font-size: 1.5rem; color: #667eea;'>→</div>", unsafe_allow_html=True)

def render_job_details(job_data):
    """Render detailed job information"""
    
    # Overview metrics
    st.markdown("### 📈 Job Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{job_data.get('status', 'unknown').upper()}</div>
            <div class="metric-label">Status</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        completed_stages = sum(1 for s in job_data['stages'].values() if s == 'done')
        total_stages = len(job_data['stages'])
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{completed_stages}/{total_stages}</div>
            <div class="metric-label">Stages Complete</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        artifacts_count = len(job_data.get('artifacts', {}))
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{artifacts_count}</div>
            <div class="metric-label">Artifacts</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        created_time = datetime.fromisoformat(job_data['created_at'].replace('Z', '+00:00'))
        elapsed = datetime.now(timezone.utc) - created_time
        elapsed_str = f"{int(elapsed.total_seconds())}s"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{elapsed_str}</div>
            <div class="metric-label">Elapsed Time</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Stages", "📁 Files", "📊 Metrics", "🔍 Raw Data"])
    
    with tab1:
        # Stage details
        st.markdown("### Stage Details")
        for stage, status in job_data['stages'].items():
            status_class = get_stage_status_class(status)
            icon = get_stage_icon(status)
            
            st.markdown(f"""
            <div class="{status_class} stage-card">
                <strong>{icon} {stage.replace('_', ' ').title()}</strong>
                <span style="float: right; font-weight: 600;">{status.upper()}</span>
            </div>
            """, unsafe_allow_html=True)
    
    with tab2:
        # MinIO Files
        st.markdown("### 📦 MinIO Files")
        
        job_id = job_data['job_id']
        files = list_minio_objects(s3_client, bucket, f"{job_id}/")
        
        if files:
            for file in files:
                filename = file['key'].split('/')[-1]
                size_mb = file['size'] / (1024 * 1024)
                
                st.markdown(f"""
                <div class="file-item">
                    <div>
                        <strong>📄 {filename}</strong><br>
                        <small style="color: #6c757d;">Size: {size_mb:.2f} MB | Modified: {file['last_modified'][:19]}</small>
                    </div>
                    <div>
                        <code>{file['key']}</code>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No files found in MinIO for this job yet.")
        
        # Artifacts from job state
        if job_data.get('artifacts'):
            st.markdown("### 🔗 Artifact URIs")
            for key, uri in job_data['artifacts'].items():
                st.code(f"{key}: {uri}", language="text")
    
    with tab3:
        # Execution Metrics
        st.markdown("### ⏱️ Execution Metrics")
        
        metrics_collections = {
            "downloads": "📥 Download",
            "splits": "✂️ Split",
            "transcriptions": "📝 Transcription",
            "audio_features": "🎵 Audio Features",
            "metadata_public": "📊 Metadata",
            "scoring": "⭐ Scoring"
        }
        
        for collection, display_name in metrics_collections.items():
            logs = list(mongo_db[collection].find({"job_id": job_data['job_id']}, {"_id": 0}))
            
            if logs:
                with st.expander(f"{display_name} Metrics", expanded=False):
                    df = pd.DataFrame(logs)
                    st.dataframe(df, width="stretch")
    
    with tab4:
        # Raw JSON data
        st.json(job_data)

def format_bytes(bytes_val):
    """Format bytes to human readable"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} TB"

# Main App
st.markdown('<h1 class="main-header">🎬 SignalForge</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Multimodal Video Processing Pipeline</p>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### ⚙️ Actions")
    
    if st.button("🔄 Refresh", width="stretch"):
        st.rerun()
    
    st.markdown("---")
    
    st.markdown("### 📊 System Stats")
    total_jobs = mongo_db["jobs"].count_documents({})
    completed_jobs = mongo_db["jobs"].count_documents({"status": "completed"})
    failed_jobs = mongo_db["jobs"].count_documents({"status": "failed"})
    
    st.metric("Total Jobs", total_jobs)
    st.metric("Completed", completed_jobs)
    st.metric("Failed", failed_jobs)

# Main content tabs
main_tab1, main_tab2, main_tab3 = st.tabs(["🚀 Submit Job", "📊 Job Status", "📜 All Jobs"])

with main_tab1:
    st.markdown("### Submit New Processing Job")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        url = st.text_input("YouTube URL or Video Path", placeholder="https://youtube.com/watch?v=...")
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        submit_button = st.button("🚀 Submit Job", type="primary", width="stretch")
    
    if submit_button and url:
        job_id = str(uuid.uuid4())
        
        # Create job in MongoDB
        job_state = create_job(mongo_db, job_id, url)
        
        # Log to ingestion_logs
        log_execution(mongo_db, "ingestion_logs", {
            "job_id": job_id,
            "url": url,
            "action": "job_submitted",
            "status": "success"
        })
        
        # Publish to RabbitMQ
        publish_job({
            "job_id": job_id,
            "url": url,
            "stage": "download"
        })
        
        st.session_state["job_id"] = job_id
        st.success(f"✅ Job submitted successfully!")
        st.code(f"Job ID: {job_id}", language="text")
        st.info("💡 Switch to 'Job Status' tab to track progress")

with main_tab2:
    st.markdown("### Track Job Progress")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        job_id_input = st.text_input(
            "Job ID", 
            value=st.session_state.get("job_id", ""),
            placeholder="Enter job ID to track..."
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        track_button = st.button("🔍 Track", type="primary", width="stretch")
    
    # Auto-refresh for active jobs
    if job_id_input:
        job_data = get_job(mongo_db, job_id_input)
        
        if job_data:
            # Auto-refresh if job is not complete
            if job_data['status'] not in ['completed', 'failed']:
                st.info("🔄 Auto-refreshing every 3 seconds...")
                time.sleep(3)
                st.rerun()
            
            # Render visualizations
            render_pipeline_visualization(job_data)
            st.markdown("---")
            render_job_details(job_data)
        else:
            st.error("❌ Job not found. Please check the Job ID.")

with main_tab3:
    st.markdown("### All Jobs")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "submitted", "completed", "failed"]
        )
    
    with col2:
        limit = st.number_input("Jobs to show", min_value=10, max_value=100, value=20, step=10)
    
    # Get jobs
    if status_filter == "All":
        jobs = get_all_jobs(mongo_db, limit=limit)
    else:
        jobs = list(mongo_db["jobs"].find(
            {"status": status_filter}, 
            {"_id": 0}
        ).sort("created_at", -1).limit(limit))
    
    if jobs:
        # Create summary table
        jobs_df = pd.DataFrame([{
            'Job ID': j['job_id'][:8] + '...',
            'URL': j['url'][:50] + '...' if len(j['url']) > 50 else j['url'],
            'Status': j['status'],
            'Progress': f"{sum(1 for s in j['stages'].values() if s == 'done')}/{len(j['stages'])}",
            'Created': j['created_at'][:19]
        } for j in jobs])
        
        st.dataframe(jobs_df, width="stretch")
        
        # Detailed view
        st.markdown("### Job Details")
        selected_job_id = st.selectbox(
            "Select a job to view details",
            [j['job_id'] for j in jobs],
            format_func=lambda x: f"{x[:12]}... ({next(j['status'] for j in jobs if j['job_id'] == x)})"
        )
        
        if selected_job_id:
            selected_job = next(j for j in jobs if j['job_id'] == selected_job_id)
            st.markdown("---")
            render_pipeline_visualization(selected_job)
            st.markdown("---")
            render_job_details(selected_job)
    else:
        st.info("No jobs found.")

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #6c757d; padding: 1rem;'>"
    "SignalForge v2.0 | Powered by MongoDB, RabbitMQ, MinIO, and Streamlit"
    "</div>",
    unsafe_allow_html=True
)
