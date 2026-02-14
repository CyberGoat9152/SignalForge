from pymongo import MongoClient
from boto3 import client
from datetime import datetime, timezone
import json

def get_mongo_client(config):
    mongo_uri = f"mongodb://{config['MONGO_USER']}:{config['MONGO_PASS']}@{config['MONGO_HOST']}:27017/"
    return MongoClient(mongo_uri)

def get_minio_client(config):
    return client(
        's3',
        endpoint_url=f"http://{config['MINIO_HOST']}:9000",
        aws_access_key_id=config['MINIO_ACCESS_KEY'],
        aws_secret_access_key=config['MINIO_SECRET_KEY']
    )

def log_execution(mongo_db, collection_name, data):
    data['logged_at'] = datetime.now(timezone.utc).isoformat()
    mongo_db[collection_name].insert_one(data)

def upload_to_minio(s3_client, bucket, key, file_path):
    s3_client.upload_file(file_path, bucket, key)
    return f"s3://{bucket}/{key}"

def download_from_minio(s3_client, bucket, key, local_path):
    s3_client.download_file(bucket, key, local_path)
