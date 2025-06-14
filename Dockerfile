FROM bitnami/spark:latest

USER root

# Instala o pip e bibliotecas necessárias
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install pyspark

# Copia o script para dentro do contêiner
COPY app.py /opt/app.py

# Comando padrão ao rodar o contêiner
CMD ["spark-submit", "/opt/app.py"]
