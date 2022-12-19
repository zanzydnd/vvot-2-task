FROM --platform=linux/amd64 python:3.11.1
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc

COPY requirements.txt .
COPY face_cut.py .

RUN pip install -r requirements.txt

CMD [ "python", "face_cut.py" ]


