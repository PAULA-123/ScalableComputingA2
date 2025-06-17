import redis
import json
from statistics import mean

REDIS_HOST = "redis"
REDIS_PORT = 6379
SOURCE_CHANNEL = "filtered_secretary"

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pubsub = r.pubsub()
    pubsub.subscribe(SOURCE_CHANNEL)
    valores = []
    print(f"[METRICA] Escutando canal: {SOURCE_CHANNEL}")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                dado = json.loads(message['data'])
                if "Diagnostico" in dado:
                    valores.append(dado["Diagnostico"])
                    if len(valores) % 1000 == 0:
                        media = mean(valores)
                        print(f"[METRICA] Média de diagnosticados (em {len(valores)}): {media:.4f}")
            except Exception as e:
                print(f"[METRICA] Erro ao processar mensagem: {e}")

if __name__ == "__main__":
    main()
