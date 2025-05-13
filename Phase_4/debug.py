from sqs_config import RESULT_QUEUE_URL, receive_messages_sqs

msgs = receive_messages_sqs(RESULT_QUEUE_URL, max_messages=10)
print("\n🔍 [DEBUG] Fetched messages from RESULT_QUEUE:")
for msg in msgs:
    print(msg)