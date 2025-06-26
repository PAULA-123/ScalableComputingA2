FROM bitnami/spark:latest

USER root

# Cache do APT e instalação otimizada em uma única layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      gcc g++ \
      python3-dev python3-pip \
      librdkafka-dev \
      build-essential pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Criar usuário com informações completas
RUN useradd -m -u 1001 -s /bin/bash sparkuser && \
    usermod -aG root sparkuser

# Configurações de ambiente
ENV HOME=/app \
    USER=sparkuser \
    JAVA_TOOL_OPTIONS="-Duser.home=/app" \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Cria estrutura de diretórios e ajusta permissões
RUN mkdir -p /app/.ivy2 /app/.cache/pip /app/logs /app/data && \
    chown -R sparkuser:sparkuser /app && \
    python3 -m pip install --upgrade pip setuptools wheel

USER sparkuser

# Resto do Dockerfile permanece igual...
COPY --chown=sparkuser:sparkuser requirements.txt .
RUN pip install --user --no-warn-script-location -r requirements.txt

COPY --chown=sparkuser:sparkuser . .

# Configuração Spark com autenticação desabilitada
ENTRYPOINT ["spark-submit", \
    "--master", "local[*]", \
    "--driver-memory", "1g", \
    "--executor-memory", "1g", \
    "--conf", "spark.sql.adaptive.enabled=true", \
    "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true", \
    "--conf", "spark.authenticate=false", \
    "--conf", "spark.network.auth.enabled=false"]
CMD ["placeholder.py"]