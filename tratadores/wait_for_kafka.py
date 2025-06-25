import socket
import time
import sys
import os

host = os.getenv("KAFKA_HOST", "kafka")
port = int(os.getenv("KAFKA_PORT", "9092"))

def esperar_kafka(host, port, timeout=60):
    inicio = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"✅ Kafka disponível em {host}:{port}")
                return
        except OSError:
            print(f"⏳ Aguardando Kafka em {host}:{port}...")
            time.sleep(2)

        if time.time() - inicio > timeout:
            print(f"❌ Timeout após {timeout}s aguardando Kafka")
            sys.exit(1)

if __name__ == "__main__":
    esperar_kafka(host, port)
    os.execvp("python", ["python"] + sys.argv[1:])
