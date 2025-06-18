FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY filtrador.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "filtrador.py"]
