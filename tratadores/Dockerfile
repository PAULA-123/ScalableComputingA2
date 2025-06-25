FROM bitnami/spark:3.3.2

# Diretório de trabalho
WORKDIR /app

# Copia apenas o requirements primeiro (melhor aproveitamento de cache)
COPY requirements.txt .

# Instala dependências antes de copiar o restante do código
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Garante logs em tempo real
ENV PYTHONUNBUFFERED=1

# Comando de execução
CMD ["python", "pipeline.py"]
