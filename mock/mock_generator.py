import redis
import json
import time
import random
from datetime import datetime, timedelta

# Conexão com o Redis (nome do serviço no docker-compose)
r = redis.Redis(host='redis', port=6379)

def gerar_dado_exemplo():
    hoje = datetime.now()
    return {
        "origem": "mock",
        "timestamp": hoje.strftime("%Y-%m-%d %H:%M:%S"),
        "valor": random.randint(0, 100),
        "status": random.choice(["ok", "alerta", "erro"]),
    }

def main():
    print("🔁 Iniciando mock de publicação no Redis...")

    for i in range(10):  # Envia 10 mensagens
        dado = gerar_dado_exemplo()
        r.publish("dados-saude", json.dumps(dado))
        print(f"📤 Publicado: {dado}")
        time.sleep(1)

    print("✅ Publicação finalizada.")

if __name__ == "__main__":
    main()
