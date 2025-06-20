FROM bitnami/spark:latest

# 1. Executa como root temporariamente para configurações do sistema
USER root

# 2. Cria diretórios necessários com permissões adequadas
RUN install_packages apt-transport-https ca-certificates && \
    mkdir -p /var/lib/apt/lists/partial && \
    mkdir -p /var/cache/apt/archives/partial && \
    chown -R 1001:1001 /var/lib/apt /var/cache/apt && \
    chmod -R 775 /var/lib/apt /var/cache/apt

# 3. Instala dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    librdkafka-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 4. Configura o ambiente de trabalho
WORKDIR /app
RUN chown -R 1001:1001 /app

# 5. Volta para o usuário não-root padrão (1001)
USER 1001

# 6. Copia e instala dependências Python
COPY --chown=1001:1001 requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 7. Copia o código fonte
COPY --chown=1001:1001 metrica.py .

# 8. Comando de execução
CMD ["spark-submit", "--master", "local[*]", "/app/metrica.py"]