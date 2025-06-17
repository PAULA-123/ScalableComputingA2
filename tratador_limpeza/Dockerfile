FROM python:3.11-slim
WORKDIR /app
COPY cleaner.py .
RUN pip install redis
CMD ["python", "cleaner.py"]
