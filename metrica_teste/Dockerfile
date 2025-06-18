# FROM bitnami/spark:latest

# # Copia os arquivos de código e dados
# COPY metrica_teste.py /app/metrica_teste.py
# COPY dados.json /app/dados.json

# # Define o diretório de trabalho
# WORKDIR /app

# # Cria a pasta /tmp/artifacts com permissão total para evitar erro
# RUN mkdir -p /tmp/artifacts && chmod -R 777 /tmp/artifacts

# RUN mkdir -p /app/artifacts && chmod -R 777 /app/artifacts


# # Também cria um diretório local spark-local (opcional)
# RUN mkdir -p /tmp/spark-local && chmod 777 /tmp/spark-local

# # Define variável de ambiente para Spark usar /tmp/artifacts como local dir
# ENV SPARK_LOCAL_DIRS=/tmp/artifacts

# # Comando para rodar o script
# CMD ["/opt/bitnami/spark/bin/spark-submit", "metrica_teste.py"]

FROM bitnami/spark:latest

USER root
RUN mkdir -p /app/artifacts && chmod -R 777 /app/artifacts

WORKDIR /app

COPY metrica_teste.py /app/
COPY dados.json /app/

# NÃO mude de usuário aqui para 1001 a menos que tenha certeza
# USER 1001

CMD ["/opt/bitnami/spark/bin/spark-submit", "metrica_teste.py"]

 