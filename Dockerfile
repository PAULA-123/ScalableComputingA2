FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y python3-pip
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

COPY . /app
WORKDIR /app
