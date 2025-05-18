# === sqs_config.py (updated) ===
import boto3
import json
import hashlib
import os

# ===============================
# AWS Clients
# ===============================
AWS_REGION = 'eu-north-1'
AWS_ACCESS_KEY = 'AKIAWS4MJEGYQU7JZNMK'
AWS_SECRET_KEY = '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'

sqs = boto3.client('sqs', region_name=AWS_REGION,
                   aws_access_key_id=AWS_ACCESS_KEY,
                   aws_secret_access_key=AWS_SECRET_KEY)

s3 = boto3.client('s3', region_name=AWS_REGION,
                  aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY)

# ===============================
# Hardcoded Queue URLs and S3
# ===============================
DASHBOARD_HOST = "http://192.168.1.12:5000"

CRAWL_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/452876378545/CrawlQueue.fifo'
HEARTBEAT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/452876378545/HeartBeatQueue'
RESULT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/452876378545/ResultQueue'

S3_BUCKET_NAME = 'ahmed-crawler-bucket'

# ===============================
# SQS Helper Functions
# ===============================
def send_message_sqs(queue_url, message_type, payload, group_id="default"):
    message_body = {"type": message_type, "payload": payload}
    message = {"QueueUrl": queue_url, "MessageBody": json.dumps(message_body)}
    if queue_url.endswith(".fifo"):
        message["MessageGroupId"] = group_id
    response = sqs.send_message(**message)
    return response

def receive_messages_sqs(queue_url, max_messages=10, wait_time=5):
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=max_messages, WaitTimeSeconds=wait_time)
    messages = response.get('Messages', [])
    parsed_messages = []
    for msg in messages:
        body = json.loads(msg['Body'])
        parsed_message = {"type": body.get("type"), "payload": body.get("payload"), "receipt_handle": msg['ReceiptHandle']}
        parsed_messages.append(parsed_message)
    return parsed_messages

def delete_parsed_message_sqs(queue_url, parsed_message):
    receipt_handle = parsed_message.get("receipt_handle")
    if receipt_handle:
        return sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

# ===============================
# S3 Helper Functions
# ===============================
def upload_to_s3(content, key, metadata=None):
    response = s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=content.encode("utf-8") if isinstance(content, str) else content,
        Metadata=metadata or {}
    )
    return response

def url_to_s3_key(url):
    hashed = hashlib.md5(url.encode()).hexdigest()
    return f"{hashed}.html"

def download_from_s3(key):
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    body = obj['Body'].read().decode('utf-8')
    url = obj['Metadata'].get("original_url", key)
    return url, body