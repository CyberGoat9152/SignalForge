# SignalForge (working name)

SignalForge is an event-driven multimodal pipeline for refining raw video into ranked high-attention segments.

The system ingests long-form video, extracts structured signals from multiple modalities (audio, text, video, metadata), and produces a ranked list of candidate clips suitable for shorts, highlights, or downstream content generation.

This is not a “single AI model”, but a distributed information refinery composed of specialized workers.

High-level idea

Raw video is treated as unstructured data.

SignalForge transforms it into:

- aligned audio, video and text streams,

- low-level perceptual features,

- engagement-oriented signals,

- and finally ranked temporal segments.

The core design principle is:

> Signals first, intelligence last.

Feature extraction is separated from decision making.
Workers never decide “what is good”, they only describe reality.
Only the final scoring stage performs ranking.

# Architecture overview

The system is built as a distributed state machine:

- RabbitMQ is used for control flow (events).

- Redis is used as the single source of truth (state).

- Object storage / filesystem stores artifacts.

Each job is identified by a `job_id` and progresses through a fixed pipeline of stages.

# Data model (Redis)

Each job is stored as:

```
job:{job_id}
```

With the following structure:

```json
{
  "job_id": "...",
  "status": "running | done",
  "stages": {
    "download": "pending | running | done",
    "split": "pending | running | done",
    "transcribe": "pending | running | done",
    "audio_features": "pending | running | done",
    "metadata_public": "pending | running | done",
    "scoring": "pending | running | done"
  },
  "artifacts": {
    "raw_video": "...",
    "audio": "...",
    "video": "...",
    "transcript": "..."
  },
  "features": {
    "audio": [...],
    "video": [...],
    "text": [...]
  },
  "metadata": {
    "public": {...}
  },
  "scores": {
    "segments": [...]
  }
}
```

Redis is the canonical state.
Rabbit never stores truth, only transitions.

# Worker chain (control flow)

The system uses one queue per stage.
```
q.download
   ↓
q.split
   ↓
q.transcribe
   ↓
q.audio_features
   ↓
q.metadata_public
   ↓
q.scoring
```

Each worker:

1. Consumes from its queue.
2. Reads job:{job_id} from Redis.
3. Sets its stage = running.
4. Performs its task.
5. Writes artifacts / features.
6. Sets its stage = done.
7. Publishes to the next queue.

No worker calls another worker directly.
All coordination is implicit through Redis state + Rabbit events.

# Workers
1. Downloader

    Input:
    - YouTube URL or local path.

    Output:
    - raw video file.

    Responsibility:
    - Normalize ingestion.
    - Persist original artifact.

2. Splitter

    Input:
    - raw video.

    Output:
    - audio.wav
    - video.mp4 (no audio)

    Responsibility:
    - Separate modalities.
    - Normalize formats (fps, codec, sample 
    rate).

3. Transcriber

    Input:
    - audio.wav

    Output:
    - transcript (Whisper JSON)

    Responsibility:
    - Produce time-aligned text.
    - No interpretation or scoring.

4. Audio feature extractor

    Input:
    - audio.wav

    Output:
    - time-series features:
    - RMS energy
    - pitch
    - zero-crossing rate
    - speaking rate

    Responsibility:
    - Describe acoustic salience.
    - No semantic meaning.

5. Public metadata worker

Input:
- video_id

Output:
- views
- likes
- comments
- views per day
- like ratio
- engagement score

Responsibility:
- Enrich job with global popularity signals.
- Used only as weighting, never as labels.

6. Scoring worker
    Input:
    - transcript
    - audio features
    - metadata

    Output:

    - ranked list of segments:
        - start time
        - end time
        - final score
    Responsibility:
        - Fuse all signals.
        - Produce decisions.

This is the only stage that is “intelligent”.

# Data flow (end-to-end)

1. Streamlit publishes job to q.download.
2. Downloader saves raw video.
3. Splitter extracts audio and video.
4. Transcriber generates text.
5. Audio worker extracts acoustic features.
6. Metadata worker fetches public metrics.
7. Scoring worker fuses everything and ranks segments.
8. Streamlit visualizes and collects feedback.

# Design principles
## Event-driven

No service calls another directly.
Everything is asynchronous and loosely coupled.

### Idempotent

Any worker can be restarted safely.
State always comes from Redis.

### Observable

At any moment, the full system state can be reconstructed from Redis.

### Extensible

New workers can be inserted without breaking old ones:

- video_features
- emotion detection
- clip export
- user feedback ingestion
- What this system is (and is not)

This system is:

- a multimodal information refinery,
- a signal extraction platform,
- a distributed ML pipeline.

This system is not:

- a single neural network,
- an end-to-end black box,
- a “viral content predictor”.

Real learning only happens when private analytics (retention curves) are added. Until then, this is a high-quality expert system with weak supervision.

# Mental model

SignalForge is best understood as:

> A factory that converts unstructured audiovisual chaos into structured attention signals.a