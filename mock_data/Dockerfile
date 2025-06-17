FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "generator.py"]
