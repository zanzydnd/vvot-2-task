import base64
import io
import json
import logging
import os
import requests
import boto3


logger = logging.getLogger(__name__)
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


