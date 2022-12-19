import io
import json
import logging
import os
import uuid
import ydb
import boto3

# from dotenv import load_dotenv, find_dotenv
from PIL import Image
from sanic import Sanic
from sanic.response import text

logger = logging.getLogger(__name__)
# load_dotenv(find_dotenv())
BUCKET_ENDPOINT_URL = os.environ.get("BUCKET_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
REGION_NAME = os.environ.get("REGION_NAME")
VISION_TOKEN = os.environ.get("VISION_TOKEN")
ACCESS_KEY_FACES = os.environ.get("ACCESS_KEY_FACES")
SECRET_KEY_FACES = os.environ.get("SECRET_KEY_FACES")
BUCKET_FACES_NAME = os.environ.get("BUCKET_FACES_NAME")
YDB_DATABASE = os.environ.get("YDB_DATABASE")
YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT")


def handle_message(message: dict) -> dict:
    _ = json.loads(message.get("messages")[0].get("details").get("message").get("body"))
    logger.warning(_)
    return _


def download_photo(bucket_id, object_id: str):
    session = boto3.session.Session()
    client = session.client(
        service_name="s3",
        endpoint_url=BUCKET_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME
    )

    with open('file.jpg', 'wb') as f:
        client.download_fileobj(bucket_id, object_id, f)
    logger.warning("downloaded")

def get_rectangle(coords: list):
    logger.warning(f'rectangle {[coords[0].get("x"), coords[0].get("y"), coords[3].get("x"), coords[3].get("y")]}')
    return [coords[0].get("x"), coords[0].get("y"), coords[3].get("x"), coords[3].get("y")]


def cut_faces(faces: list):
    img = Image.open('file.jpg')
    cropped_faces = []
    for face in faces:
        cropped_faces.append(img.crop(get_rectangle(face.get("vertices"))))

    img.close()
    logger.warning("cutted")
    return cropped_faces


def execute_query(session, **kwargs):
    # Create the transaction and execute query.
    logger.warning('executing qujery')
    return session.transaction().execute(
        f"INSERT INTO main (face_photo_id, orig_photo_id) VALUES ('{kwargs.get('face_photo_id')}','{kwargs.get('orig_photo_id')}');",
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )


def push_faces(faces: list, object_id: str):
    # Create driver in global space.
    driver = ydb.Driver(endpoint=YDB_ENDPOINT, database=YDB_DATABASE)
    # Wait for the driver to become active for requests.
    driver.wait(fail_fast=True, timeout=5)
    # Create the session pool instance to manage YDB sessions.
    pool = ydb.SessionPool(driver)

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_FACES,
                      aws_secret_access_key=SECRET_KEY_FACES)
    i = 1
    for img in faces:
        in_mem_file = io.BytesIO()
        img.save(in_mem_file, format="JPEG")
        obj_in_faces_bucket_name = f"face_{i}_" + object_id + f"-{uuid.uuid4()}.jpeg"
        logger.warning(obj_in_faces_bucket_name)
        s3.upload_file(in_mem_file.getvalue(), BUCKET_FACES_NAME, obj_in_faces_bucket_name)
        del in_mem_file
        pool.retry_operation_sync(execute_query, orig_photo_id=object_id, face_photo_id=obj_in_faces_bucket_name)
        i += 1


app = Sanic(__name__)


@app.route("/", methods=["GET", "POST"])
async def main_route(request):
    message = handle_message(request.body.decode("utf-8"))
    download_photo(message.get('bucket_id'), message.get('bucket_obj_id'))

    faces = cut_faces(message.get("faces"))
    push_faces(faces, message.get('bucket_obj_id'))
    return text("ok")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 80), motd=False, access_log=False)
