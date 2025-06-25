FROM bitnami/spark:latest

USER root

# Instalar pacotes essenciais e librdkafka para Python confluent-kafka (se ainda usar)
RUN install_packages apt-transport-https ca-certificates && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        python3-dev \
        librdkafka-dev \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN chown -R 1001:1001 /app

USER 1001

# Copia requirements.txt e instala dependências Python (incluindo pyspark)
COPY --chown=1001:1001 requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código Python do cleaner
COPY --chown=1001:1001 cleaner.py .

# Spark Structured Streaming já tem o conector Kafka incluído no bitnami/spark,
# mas certifique-se que pyspark no requirements.txt é compatível (ex: pyspark>=3.0).

CMD ["spark-submit", "--master", "local[*]", "/app/cleaner.py"]
