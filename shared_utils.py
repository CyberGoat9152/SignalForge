from pymongo import MongoClient
from boto3 import client
from datetime import datetime, timezone
import json
import os

def get_mongo_client(config):
    mongo_uri = f"mongodb://{config['MONGO_USER']}:{config['MONGO_PASS']}@{config['MONGO_HOST']}:27017/?authSource=admin"
    return MongoClient(mongo_uri)

def get_minio_client(config):
    return client(
        's3',
        endpoint_url=f"http://{config['MINIO_HOST']}:9000",
        aws_access_key_id=config['MINIO_ACCESS_KEY'],
        aws_secret_access_key=config['MINIO_SECRET_KEY']
    )

def log_execution(mongo_db, collection_name, data):
    """Log execution metrics to MongoDB"""
    data['logged_at'] = datetime.now(timezone.utc).isoformat()
    mongo_db[collection_name].insert_one(data)

def upload_to_minio(s3_client, bucket, key, file_path):
    """Upload file to MinIO and return S3 URI"""
    s3_client.upload_file(file_path, bucket, key)
    return f"s3://{bucket}/{key}"

def download_from_minio(s3_client, bucket, key, local_path):
    """Download file from MinIO"""
    s3_client.download_file(bucket, key, local_path)

def download_from_minio_direct(s3_client, bucket, key, local_path):
    """Download without atomic rename to avoid permission issues"""
    os.makedirs(os.path.dirname(local_path) or '.', exist_ok=True)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    with open(local_path, 'wb') as f:
        for chunk in response['Body'].iter_chunks(chunk_size=8192):
            f.write(chunk)

def create_job(mongo_db, job_id, url):
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
    mongo_db["jobs"].insert_one(job_state)
    return job_state

def get_job(mongo_db, job_id):
    return mongo_db["jobs"].find_one({"job_id": job_id}, {"_id": 0})

def update_job_stage(mongo_db, job_id, stage, status):
    mongo_db["jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                f"stages.{stage}": status,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )

def update_job_status(mongo_db, job_id, status):
    mongo_db["jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )

def update_job_artifacts(mongo_db, job_id, artifact_key, artifact_value):
    mongo_db["jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                f"artifacts.{artifact_key}": artifact_value,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )

def update_job_features(mongo_db, job_id, feature_type, features):
    mongo_db["jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                f"features.{feature_type}": features,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )

def update_job_metadata(mongo_db, job_id, metadata_type, metadata):
    mongo_db["jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                f"metadata.{metadata_type}": metadata,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )

def update_job_scores(mongo_db, job_id, scores):
    mongo_db["jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                "scores": scores,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )

def set_job_error(mongo_db, job_id, stage, error_message):
    mongo_db["jobs"].update_one(
        {"job_id": job_id},
        {
            "$set": {
                f"stages.{stage}": "failed",
                "status": "failed",
                "error": error_message,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        }
    )

def get_all_jobs(mongo_db, limit=50):
    return list(mongo_db["jobs"].find({}, {"_id": 0}).sort("created_at", -1).limit(limit))

def list_minio_objects(s3_client, bucket, prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in response:
            return [{
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat()
            } for obj in response['Contents']]
        return []
    except Exception as e:
        print(f"Error listing MinIO objects: {e}")
        return []
