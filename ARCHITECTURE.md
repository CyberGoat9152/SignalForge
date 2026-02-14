# SignalForge Architecture

## System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                         │
│                                                                 │
│  ┌──────────────┐                                               │
│  │  Streamlit   │  ← User submits YouTube URL                   │
│  │      UI      │  → Publishes to q.download                    │
│  └──────────────┘  → Logs to MongoDB.ingestion_logs             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        PROCESSING LAYER                         │
│                                                                 │
│  [q.download] → Download Worker                                 │
│    ↓ Downloads raw video via yt-dlp                             │
│    ↓ Uploads to MinIO: s3://signalforge/{job_id}/raw.mp4        │
│    ↓ Logs: file_size, execution_time → MongoDB.downloads        │
│    ↓ Updates Redis: job:{job_id}.stages.download = "done"       │
│    └→ Publishes to q.split                                      │
│                                                                 │
│  [q.split] → Split Worker                                       │
│    ↓ Downloads raw video from MinIO                             │
│    ↓ FFmpeg: Extracts audio.wav + video.mp4                     │
│    ↓ Uploads to MinIO:                                          │
│    │   - s3://signalforge/{job_id}/audio.wav                    │
│    │   - s3://signalforge/{job_id}/video.mp4                    │
│    ↓ Logs: sizes, execution_time → MongoDB.splits               │
│    ├→ Publishes to q.transcribe                                 │
│    └→ Publishes to q.audio_features (parallel)                  │
│                                                                 │
│  [q.transcribe] → Transcribe Worker                             │
│    ↓ Downloads audio from MinIO                                 │
│    ↓ Whisper: Generates transcript.json                         │
│    ↓ Uploads to MinIO: s3://signalforge/{job_id}/transcript.json│
│    ↓ Logs: word_count, execution_time → MongoDB.transcriptions  │
│    └→ Publishes to q.metadata_public                            │
│                                                                 │
│  [q.audio_features] → Audio Features Worker (parallel)          │
│    ↓ Downloads audio from MinIO                                 │
│    ↓ Librosa: Extracts RMS, ZCR, pitch (2s windows)             │
│    ↓ Stores features in Redis job state                         │
│    ↓ Logs: feature_count, duration → MongoDB.audio_features     │
│    └→ Publishes to q.scoring                                    |
│                                                                 │
│  [q.metadata_public] → Metadata Worker                          │
│    ↓ yt-dlp --dump-json (no download)                           │
│    ↓ Extracts: views, likes, comments, engagement_score         │
│    ↓ Stores metadata in Redis job state                         │
│    ↓ Logs: view_count, engagement → MongoDB.metadata_public     │
│    └→ Publishes to q.scoring                                    |
│                                                                 |  
│  [q.scoring] → Scoring Worker (waits for all signals)           │
│    ↓ Reads from Redis:                                          │
│    │   - features.audio[]                                       │
│    │   - metadata.public{}                                      │
│    ↓ Signal fusion algorithm:                                   │
│    │   score = (RMS×0.4 + ZCR×0.3 + pitch×0.3) × engagement     │
│    ↓ Ranks segments, stores top 50                              │
│    ↓ Logs: top_score, avg_score → MongoDB.scoring               │
│    └→ Updates Redis: job.status = "completed"                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                               │
│                                                                     │
│  Redis (State)           MongoDB (Metrics)        MinIO (Artifacts) │
│  ┌──────────────┐       ┌────────────────┐      ┌────────────────┐  │
│  │ job:{id}     │       │ downloads      │      │ raw.mp4        │  │
│  │  - status    │       │ splits         │      │ audio.wav      │  │
│  │  - stages    │       │ transcriptions │      │ video.mp4      │  │
│  │  - artifacts │       │ audio_features │      │ transcript.json|  │
│  │  - features  │       │ metadata_public│      └────────────────┘  │
│  │  - metadata  │       │ scoring        │                          │
│  │  - scores    │       │ ingestion_logs │                          │
│  └──────────────┘       └────────────────┘                          │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow Example

### Job: "abc-123"

1. **Ingestion** (t=0s)
   ```json
   Redis: job:abc-123 = {
     "status": "submitted",
     "stages": {"download": "pending", ...}
   }
   MongoDB.ingestion_logs: {
     "job_id": "abc-123",
     "url": "youtube.com/watch?v=xyz",
     "action": "job_submitted"
   }
   ```

2. **Download** (t=5s)
   ```json
   MinIO: s3://signalforge/abc-123/raw.mp4 (150 MB)
   Redis: job:abc-123.stages.download = "done"
   MongoDB.downloads: {
     "job_id": "abc-123",
     "file_size_bytes": 157286400,
     "execution_time_seconds": 5.2
   }
   ```

3. **Split** (t=12s)
   ```json
   MinIO:
     - s3://signalforge/abc-123/audio.wav (12 MB)
     - s3://signalforge/abc-123/video.mp4 (145 MB)
   Redis: job:abc-123.stages.split = "done"
   MongoDB.splits: {
     "audio_size_bytes": 12582912,
     "video_size_bytes": 152043520,
     "execution_time_seconds": 7.1
   }
   ```

4. **Transcribe** (t=45s, parallel with Audio Features)
   ```json
   MinIO: s3://signalforge/abc-123/transcript.json
   Redis: job:abc-123.artifacts.transcript = "s3://..."
   MongoDB.transcriptions: {
     "word_count": 1250,
     "execution_time_seconds": 33.4
   }
   ```

5. **Audio Features** (t=38s, parallel with Transcribe)
   ```json
   Redis: job:abc-123.features.audio = [
     {"start": 0, "end": 2, "rms": 0.45, "zcr": 0.12, "pitch": 220},
     {"start": 2, "end": 4, "rms": 0.52, ...},
     ...
   ]
   MongoDB.audio_features: {
     "feature_count": 150,
     "duration_seconds": 300,
     "execution_time_seconds": 26.2
   }
   ```

6. **Metadata** (t=48s)
   ```json
   Redis: job:abc-123.metadata.public = {
     "view_count": 1250000,
     "like_count": 45000,
     "engagement_score": 0.036
   }
   MongoDB.metadata_public: {
     "view_count": 1250000,
     "engagement_score": 0.036,
     "execution_time_seconds": 3.1
   }
   ```

7. **Scoring** (t=52s, waits for all signals)
   ```json
   Redis: job:abc-123.scores.segments = [
     {"start": 120, "end": 122, "final_score": 0.89},
     {"start": 45, "end": 47, "final_score": 0.84},
     ...
   ]
   Redis: job:abc-123.status = "completed"
   MongoDB.scoring: {
     "segment_count": 150,
     "top_score": 0.89,
     "avg_score": 0.42,
     "execution_time_seconds": 4.2
   }
   ```

## MongoDB Collections Schema

### downloads
```javascript
{
  job_id: "abc-123",
  url: "youtube.com/watch?v=xyz",
  s3_uri: "s3://signalforge/abc-123/raw.mp4",
  file_size_bytes: 157286400,
  execution_time_seconds: 5.2,
  status: "success",
  logged_at: "2025-02-13T00:00:00Z"
}
```

### splits
```javascript
{
  job_id: "abc-123",
  audio_uri: "s3://signalforge/abc-123/audio.wav",
  video_uri: "s3://signalforge/abc-123/video.mp4",
  audio_size_bytes: 12582912,
  video_size_bytes: 152043520,
  execution_time_seconds: 7.1,
  status: "success",
  logged_at: "2025-02-13T00:00:07Z"
}
```

### transcriptions
```javascript
{
  job_id: "abc-123",
  transcript_uri: "s3://signalforge/abc-123/transcript.json",
  word_count: 1250,
  execution_time_seconds: 33.4,
  status: "success",
  logged_at: "2025-02-13T00:00:40Z"
}
```

### audio_features
```javascript
{
  job_id: "abc-123",
  feature_count: 150,
  duration_seconds: 300,
  execution_time_seconds: 26.2,
  status: "success",
  logged_at: "2025-02-13T00:00:33Z"
}
```

### metadata_public
```javascript
{
  job_id: "abc-123",
  video_id: "xyz",
  view_count: 1250000,
  engagement_score: 0.036,
  execution_time_seconds: 3.1,
  status: "success",
  logged_at: "2025-02-13T00:00:45Z"
}
```

### scoring
```javascript
{
  job_id: "abc-123",
  segment_count: 150,
  top_score: 0.89,
  avg_score: 0.42,
  execution_time_seconds: 4.2,
  status: "success",
  logged_at: "2025-02-13T00:00:49Z"
}
```

## Queue Flow

```
q.download
    ↓
q.split
    ↓──────────────┐
q.transcribe   q.audio_features
    ↓              ↓
q.metadata_public  │
    ↓──────────────┘
    q.scoring
```

Each worker:
1. Consumes from its queue
2. Updates Redis: stage = "running"
3. Performs work
4. Logs metrics to MongoDB
5. Updates Redis: stage = "done"
6. Publishes to next queue
7. ACKs message
