import base64
import io
import json
import logging
import os
import requests
import boto3

from dotenv import load_dotenv, find_dotenv

# AWS_SECRET_ACCESS_KEY=YCMmVVy_6mcpb61o1ZQ-p7vLWZL6sOakZMbXDAyI
# AWS_ACCESS_KEY_ID=YCAJElyWX0rraJUn1sk0qh2-8
logger = logging.getLogger(__name__)
load_dotenv(find_dotenv())
BUCKET_ENDPOINT_URL = os.environ.get("BUCKET_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
REGION_NAME = os.environ.get("REGION_NAME")
VISION_TOKEN = os.environ.get("VISION_TOKEN")

FACE_FINDER_PATH = "https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze"


def encode_file(file):
    file_content = file.getvalue()
    return base64.b64encode(file_content).decode("utf-8")


def get_file_from_bucket(bucket_id: str, object_id: str):
    session = boto3.session.Session()
    client = session.client(
        service_name="s3",
        endpoint_url=BUCKET_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME
    )

    in_mem_file = io.BytesIO()

    client.download_fileobj(bucket_id, object_id, in_mem_file)

    return in_mem_file


def prepare_faces_for_tasks_queue(response_from_yandex_vision: dict, bucket_obj_id: str, bucket_id: str):
    faces = [
        {
            "vertices": face.get("boundingBox").get("vertices")
        }
        for face in response_from_yandex_vision.get("results")[0].get("results")[0].get("faceDetection").get("faces")
    ]

    return {
        "bucket_obj_id": bucket_obj_id,
        "bucket_id": bucket_id,
        "faces": faces
    }


def handler(event, context):
    params_input = {'event': event, 'context': context}
    logger.warning(f"Got this params: {json.dumps(params_input, default=vars)}")

    bucket_id = event.get("messages")[0].get('details').get('bucket_id')
    object_id = event.get("messages")[0].get('details').get('object_id')

    in_mem_file = get_file_from_bucket(bucket_id, object_id)

    file_content = encode_file(in_mem_file)

    body = {
        "folderId": "b1gmgk1n7jlhjq8gq4oq",
        "analyze_specs": [{
            "content": file_content,
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    }

    print(body)

    response_from_vision = requests.post(
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {VISION_TOKEN}"
        },
        json=body,
        url=FACE_FINDER_PATH
    )

    logger.warning(f"Response from vision: {response_from_vision.json()}")

    to_task_queue_obj = prepare_faces_for_tasks_queue(response_from_vision.json(), object_id, bucket_id)
    put_to_queue(to_task_queue_obj)


def put_to_queue(to_task_queue_obj: dict):
    queue_client = boto3.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    queue_client.send_message(
        QueueUrl="https://message-queue.api.cloud.yandex.net/b1g71e95h51okii30p25/dj600000000al2nb02mk/vvot06-tasks",
        MessageBody=json.dumps(to_task_queue_obj)
    )
    logger.warning("Message sent")


if __name__ == '__main__':
    handler(
        event={
            "messages": [{"event_metadata": {"event_id": "e94366f0-3b1a-4fae-bcef-1cfe34276768",
                                             "event_type": "yandex.cloud.events.storage.ObjectCreate",
                                             "created_at": "2022-12-17T12:56:21.424389236Z",
                                             "tracing_context": {"trace_id": "f52f3246b4f3634c", "span_id": "",
                                                                 "parent_span_id": ""},
                                             "cloud_id": "b1g71e95h51okii30p25", "folder_id": "b1gmgk1n7jlhjq8gq4oq"},
                          "details": {"bucket_id": "itis-2022-2023-vvot06-photos", "object_id": "avatar.jpg"}}]
        },
        context={"function_name": "d4erd9pbdn1gjgdflura", "function_version": "d4etamrkdqn1v83ktjba",
                 "invoked_function_arn": "d4erd9pbdn1gjgdflura", "memory_limit_in_mb": 128,
                 "request_id": "276af334-a466-4550-ac3e-14d358be6d2e", "log_group_name": "ckgqr39coja86239kbqv",
                 "log_stream_name": "d4etamrkdqn1v83ktjba", "deadline_ms": 1671281785903, "token": {
                "access_token": "t1.9euelZqNio2dk5uXjIqbypGZzsmbju3rnpWanp6Nx4ySiZWXy5zOkZKLjo_l8_djDwlj-e9Edh40_N3z9yM-BmP570R2HjT8.ov2E9R_iw2GqaPibHXPrVhsOnZKeNYU4ZerHMstb_Y-bt3Am0T3hSrB55tlPRySFjlRHLLCrVSrpl-76xXzaCg",
                "expires_in": 41186, "token_type": "Bearer"}, "aws_request_id": "276af334-a466-4550-ac3e-14d358be6d2e"}
    )
