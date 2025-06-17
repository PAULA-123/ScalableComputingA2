FROM bitnami/spark:latest

USER root
WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "consumer_pubsub.py"]
