FROM python:3.11-slim

WORKDIR /app

COPY filtrador.py .

RUN pip install redis

CMD ["python", "filtrador.py"]
