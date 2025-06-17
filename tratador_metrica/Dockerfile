FROM python:3.11-slim

WORKDIR /app

COPY metrica.py .

RUN pip install redis

CMD ["python", "metrica.py"]
