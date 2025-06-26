FROM bitnami/spark:latest

# 1. Executa como root para preparar ambiente
USER root

# 2. Instala dependências nativas
RUN install_packages apt-transport-https ca-certificates && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    librdkafka-dev \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# 3. Define diretório de trabalho
WORKDIR /app
RUN chown -R 1001:1001 /app

# 4. Volta para usuário padrão
USER 1001

# 5. Instala dependências Python
COPY --chown=1001:1001 requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copia o código
COPY --chown=1001:1001 agrupador.py .

# 7. Executa o script com Spark
CMD ["spark-submit", "--master", "local[*]", "/app/agrupador.py"]