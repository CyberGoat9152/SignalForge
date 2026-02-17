# SignalForge v2.0 - Multimodal Video Processing Pipeline

Modern video processing pipeline with MongoDB state management and beautiful Streamlit UI.

## 🚀 What's New in v2.0

### MongoDB Replaces Redis
- **Job State**: All job state now stored in MongoDB `jobs` collection
- **No More Redis**: Removed Redis dependency - MongoDB handles everything
- **Persistent Storage**: Job state persists across restarts
- **Better Queries**: Rich querying capabilities for jobs and metrics

### Modern Streamlit UI
- **Pipeline Visualization**: See job flow as interactive cards
- **Real-time Updates**: Auto-refresh for active jobs
- **File Browser**: Browse MinIO files directly in UI
- **Metrics Dashboard**: System-wide statistics and job metrics
- **Progress Bars**: Visual feedback instead of JSON dumps
- **Responsive Design**: Modern, gradient-based design

### Architecture Improvements
- **Cleaner Code**: Centralized MongoDB operations in `shared_utils.py`
- **Better Logging**: Consistent emoji-based logging across workers
- **Health Checks**: Docker compose with proper service dependencies

## 📋 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    STREAMLIT UI (Port 8501)                     │
│  • Submit jobs          • Track progress    • View files        │
│  • Pipeline viz         • Metrics           • All jobs table    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                         RABBITMQ QUEUES                         │
│  q.download → q.split → q.transcribe → q.metadata_public        │
│                    ↓                                            │
│              q.audio_features  → q.scoring                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      WORKER SERVICES                            │
│  📥 Download  │  ✂️ Split  │  📝 Transcribe  │  🎵 Audio       │
│  📊 Metadata  │  ⭐ Scoring                                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                       STORAGE LAYER                             │
│  MongoDB (Jobs + Metrics)  │  MinIO (Artifacts)                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🗄️ MongoDB Collections

### `jobs` Collection
Main collection storing all job state:
```javascript
{
  job_id: "abc-123",
  url: "https://youtube.com/watch?v=xyz",
  status: "completed",  // submitted, running, completed, failed
  stages: {
    download: "done",
    split: "done",
    transcribe: "done",
    audio_features: "done",
    metadata_public: "done",
    scoring: "done"
  },
  artifacts: {
    raw_video: "s3://signalforge/abc-123/raw.mp4",
    audio: "s3://signalforge/abc-123/audio.wav",
    video: "s3://signalforge/abc-123/video.mp4",
    transcript: "s3://signalforge/abc-123/transcript.json"
  },
  features: {
    audio: [
      {start: 0, end: 2, rms: 0.45, zcr: 0.12, pitch: 220},
      ...
    ]
  },
  metadata: {
    public: {
      video_id: "xyz",
      title: "Video Title",
      view_count: 1250000,
      engagement_score: 0.036
    }
  },
  scores: {
    segments: [
      {start: 120, end: 122, final_score: 0.89},
      ...
    ]
  },
  created_at: "2025-02-15T10:00:00Z",
  updated_at: "2025-02-15T10:05:00Z"
}
```

### Metrics Collections
Each worker logs execution metrics:
- `downloads` - Download metrics
- `splits` - Split operation metrics
- `transcriptions` - Transcription metrics
- `audio_features` - Audio analysis metrics
- `metadata_public` - Metadata fetch metrics
- `scoring` - Scoring algorithm metrics
- `ingestion_logs` - Job submission logs

## 🛠️ Setup

### Prerequisites
- Docker & Docker Compose
- Git

### Quick Start

1. **Clone and Build**
   ```bash
   # Build base images
   docker build -f Dockerfile.base -t worker-base:latest .
   docker build -f Dockerfile.download -t worker-download:latest .
   docker build -f Dockerfile.audio -t worker-audio:latest .
   docker build -f Dockerfile.transcribe -t worker-transcribe:latest .
   ```

2. **Start Services**
   ```bash
   docker-compose up -d
   ```

3. **Create MinIO Bucket**
   ```bash
   # Access MinIO at http://localhost:9001
   # Login: minioadmin / minioadmin
   # Create bucket: signalforge
   ```

4. **Access UI**
   ```
   Streamlit:   http://localhost:8501
   RabbitMQ:    http://localhost:15672  (funere / b4t4t49152)
   MinIO:       http://localhost:9001   (minioadmin / minioadmin)
   MongoDB:     localhost:27017          (funere / b4t4t49152)
   ```

## 📊 Using the UI

### Submit Job Tab
1. Enter YouTube URL or video path
2. Click "Submit Job"
3. Copy the Job ID
4. Switch to "Job Status" tab

### Job Status Tab
1. Paste Job ID
2. Click "Track" or wait for auto-refresh
3. Watch pipeline visualization update in real-time
4. Explore tabs:
   - **Stages**: See stage-by-stage progress
   - **Files**: Browse MinIO artifacts
   - **Metrics**: View execution metrics
   - **Raw Data**: Inspect full JSON

### All Jobs Tab
- Filter by status (all/submitted/completed/failed)
- View summary table
- Click any job to see details

## 📁 File Structure

```
signalforge/
├── shared_utils.py              # MongoDB utilities (NO Redis!)
├── docker-compose.yml           # All services + MongoDB
├── ingestion/
│   ├── main.py                  # Modern Streamlit UI
│   └── config.yaml
├── worker_download/
│   ├── worker_download.py       # Download worker
│   └── config.yaml
├── worker_split/
│   ├── worker_split.py          # Split worker
│   └── config.yaml
├── worker_transcribe/
│   ├── worker_transcribe.py     # Transcribe worker
│   └── config.yaml
├── worker_audio/
│   ├── worker_audio_features.py # Audio analysis worker
│   └── config.yaml
├── worker_metadata/
│   ├── worker_metadata_public.py # Metadata worker
│   └── config.yaml
└── worker_scoring/
    ├── worker_scoring.py        # Scoring worker
    └── config.yaml
```

## 🔧 Configuration

All workers use MongoDB instead of Redis. Key config parameters:

```yaml
RABBIT_URL: "amqp://funere:b4t4t49152@192.168.0.39:5672/"
MONGO_HOST: "192.168.0.39"
MONGO_USER: "funere"
MONGO_PASS: "batata9152"
MONGO_DB: "signalforge"
MINIO_HOST: "192.168.0.39"
MINIO_ACCESS_KEY: "1hJRTlAl52DzNNHGtSxv"
MINIO_SECRET_KEY: "jQndo1bebPX7MhMlmPCGe1Z3skOxeXnzLgUenM1C"
MINIO_BUCKET: "signalforge"
```

## 📈 Monitoring

### MongoDB Queries

```javascript
// Get all jobs
db.jobs.find()

// Get completed jobs
db.jobs.find({status: "completed"})

// Get job by ID
db.jobs.findOne({job_id: "abc-123"})

// Get execution metrics
db.downloads.find().sort({logged_at: -1}).limit(10)
db.scoring.find({status: "failed"})

// Aggregate stats
db.jobs.aggregate([
  {$group: {_id: "$status", count: {$sum: 1}}}
])
```

### RabbitMQ Management
- Access: http://localhost:15672
- Monitor queue depths
- View message rates
- Inspect messages

### MinIO Console
- Access: http://localhost:9001
- Browse uploaded files
- Check storage usage
- Download artifacts

## 🚨 Troubleshooting

### Job stuck in "running"
- Check worker logs: `docker-compose logs worker_[name]`
- Check RabbitMQ queues
- Verify MongoDB connection

### Files not showing in UI
- Verify MinIO bucket exists
- Check MinIO credentials in config
- Ensure files uploaded successfully

### Worker crashes
- Check MongoDB is running: `docker-compose ps mongodb`
- Verify RabbitMQ is healthy
- Review worker logs for errors

## 🎯 Migration from v1.0 (Redis)

v1.0 used Redis for job state. v2.0 uses MongoDB exclusively.

**Key Changes:**
1. ✅ Replace `r.set()` / `r.get()` with MongoDB operations
2. ✅ Use `shared_utils` functions: `get_job()`, `update_job_stage()`, etc.
3. ✅ Remove Redis from docker-compose
4. ✅ Update all worker imports
5. ✅ New Streamlit UI with modern design

## 🎨 UI Features

### Pipeline Visualization
- **Color-coded cards**: Pending (gray), Running (yellow), Done (green), Failed (red)
- **Real-time updates**: Auto-refresh every 3 seconds for active jobs
- **Progress tracking**: X/6 stages complete

### File Browser
- **List all files**: See every artifact in MinIO
- **Size and timestamps**: File metadata at a glance
- **Copy S3 URIs**: Quick access to artifact paths

### Metrics Dashboard
- **System stats**: Total jobs, completed, failed
- **Per-job metrics**: Execution times, file sizes, counts
- **Aggregate views**: Pandas DataFrames for analysis

## 📝 License

MIT License

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Make changes (especially to UI!)
4. Test with docker-compose
5. Submit pull request

## 💡 Future Enhancements

- [ ] Add job cancellation
- [ ] Implement job retry logic
- [ ] Add email notifications
- [ ] Create REST API
- [ ] Add authentication
- [ ] Export results to CSV/Excel
- [ ] Advanced filtering in UI
- [ ] Job comparison view

---

**SignalForge v2.0** - Powered by MongoDB, RabbitMQ, MinIO, and Streamlit
