FROM bitnami/spark:latest

USER root

RUN install_packages apt-transport-https ca-certificates && \
    apt-get update && \
    apt-get install -y gcc g++ python3-dev librdkafka-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN chown -R 1001:1001 /app

USER 1001

COPY --chown=1001:1001 requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=1001:1001 filtrador.py .


# CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", "/app/filtrador.py"]
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", "/app/filtrador.py"]



