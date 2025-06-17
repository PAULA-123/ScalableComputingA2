import redis
import json
import time

REDIS_HOST = "redis"
REDIS_PORT = 6379
SOURCE_CHANNEL = "raw_secretary"
DEST_CHANNEL = "clean_secretary"

def limpeza(dado):
    # Remove campos nulos e entradas inválidas (exemplo)
    if dado.get("Diagnostico") in [0, 1] and dado.get("Populacao", 0) > 0:
        return dado
    return None

def main():
    print("Conectando ao Redis...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    print("Subscribing ao canal:", SOURCE_CHANNEL)
    pubsub = r.pubsub()
    pubsub.subscribe(SOURCE_CHANNEL)
    print(f"Escutando canal: {SOURCE_CHANNEL}")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                dado = json.loads(message['data'])
                dado_limpo = limpeza(dado)
                if dado_limpo:
                    r.publish(DEST_CHANNEL, json.dumps(dado_limpo))
            except Exception as e:
                print(f"Erro ao processar mensagem: {e}")

if __name__ == "__main__":
    main()
