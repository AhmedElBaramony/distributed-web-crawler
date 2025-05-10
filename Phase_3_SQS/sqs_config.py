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

s3 = boto3.client('s3')

# ===============================
# Hardcoded Queue URLs and S3
# ===============================
CRAWL_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/452876378545/CrawlQueue.fifo'
HEARTBEAT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/452876378545/HeartBeatQueue'
RESULT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/452876378545/ResultQueue'

S3_BUCKET_NAME = 'ahmed-crawler-bucket'

# ===============================
# SQS Helper Functions
# ===============================

def send_message_sqs(queue_url, message_type, payload, group_id="default"):
    
    """
    Sends a structured message to an SQS queue.
    
    Parameters:
    - queue_url (str): The full SQS queue URL.
    - message_type (str): The type/category of the message (e.g., 'crawl_task', 'index_content').
    - payload (dict): The actual content of the message.
    - group_id (str): The MessageGroupId (required for FIFO queues).
    """

    message_body = {"type": message_type, "payload": payload}
    message = {"QueueUrl": queue_url, "MessageBody": json.dumps(message_body)}
    
    # Check if FIFO queue (must have .fifo suffix)
    if queue_url.endswith(".fifo"):
        message["MessageGroupId"] = group_id
    
    response = sqs.send_message(**message)
    return response

def receive_messages_sqs(queue_url, max_messages=10, wait_time=5):
    """
    Retrieves and parses messages from an SQS queue.
    
    Parameters:
    - queue_url (str): The full SQS queue URL.
    - max_messages (int): Maximum number of messages to retrieve.
    - wait_time (int): Wait time (in seconds) for long-polling.
    
    Returns:
    - List of dictionaries containing message type, payload, and ReceiptHandle.
    """
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=max_messages, WaitTimeSeconds=wait_time)
    
    messages = response.get('Messages', [])
    
    parsed_messages = []
    for msg in messages:
        body = json.loads(msg['Body'])
        parsed_message = {"type": body.get("type"), "payload": body.get("payload"), "receipt_handle": msg['ReceiptHandle']}
        parsed_messages.append(parsed_message)
    return parsed_messages

def delete_parsed_message_sqs(queue_url, parsed_message):
    """
    Deletes a parsed message from the SQS queue using its receipt handle.

    Parameters:
    - queue_url (str): The full SQS queue URL.
    - parsed_message (dict): A single parsed message (from receive_messages_sqs).
    """
    
    receipt_handle = parsed_message.get("receipt_handle")
    if receipt_handle:
        return sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
    

# ===============================
# S3 Helper Functions
# ===============================

def upload_to_s3(content, key): # From the Crawler
    """
    Uploads a string or byte content to your S3 bucket using a specific key (filename).
    
    Parameters:
    - content (str or bytes): The actual content to upload (HTML, JSON, etc.)
    - key (str): The "filename" in S3 (e.g., 'index/abc123.html' or 'pages/home.html')
    
    Returns:
    - Response from the s3.put_object() API call (contains metadata)
    """
    response = s3.put_object(
        Bucket=S3_BUCKET_NAME,  # your bucket name
        Key=key,                # S3 object key (acts like a filename)
        Body=content            # actual content to store
    )
    return response


def url_to_s3_key(url):
    """
    Converts a URL into a safe, unique S3 object key using MD5 hashing.

    Parameters:
    - url (str): The original URL of a page (e.g., 'https://example.com/about')

    Returns:
    - str: A hashed filename like '3f8a32d1fe6f4d32a69bd84aa3b80e7d.html'
    
    Why use a hash?
    - URLs can contain slashes and symbols that aren't safe in filenames
    - This ensures uniqueness and avoids naming collisions
    """
    hashed = hashlib.md5(url.encode()).hexdigest()
    return f"{hashed}.html"


def download_from_s3(key):
    """
    Downloads an object from S3 and returns its content as a UTF-8 string.

    Parameters:
    - key (str): The S3 object key (filename) you want to download

    Returns:
    - str: The content of the file (usually HTML)
    """
    obj = s3.get_object(
        Bucket=S3_BUCKET_NAME,
        Key=key
    )
    # 'Body' is a stream-like object → read & decode to string
    return obj['Body'].read().decode('utf-8')