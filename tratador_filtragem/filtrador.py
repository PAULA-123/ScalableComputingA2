import redis
import json

REDIS_HOST = "redis"
REDIS_PORT = 6379
SOURCE_CHANNEL = "clean_secretary"
DEST_CHANNEL = "filtered_secretary"

def filtrar(dado):
    # Exemplo: só aceita CEP entre 11000 e 30999 e Diagnostico não nulo
    if 11000 <= int(dado.get("CEP", 0)) <= 30999 and dado.get("Diagnostico") is not None:
        return dado
    return None

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pubsub = r.pubsub()
    pubsub.subscribe(SOURCE_CHANNEL)
    print(f"[FILTRO] Escutando canal: {SOURCE_CHANNEL}")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                dado = json.loads(message['data'])
                dado_filtrado = filtrar(dado)
                if dado_filtrado:
                    r.publish(DEST_CHANNEL, json.dumps(dado_filtrado))
            except Exception as e:
                print(f"[FILTRO] Erro ao processar mensagem: {e}")

if __name__ == "__main__":
    main()
