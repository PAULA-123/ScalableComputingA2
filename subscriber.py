import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0)
STREAM_KEY = "data_stream"
PUBSUB_CHANNEL = "process_trigger"

def process_message(msg_id, message):
    origin = message[b'origin'].decode()
    stage = message[b'stage'].decode()
    payload = json.loads(message[b'payload'].decode())
    print(f"Processando {msg_id} | origin: {origin} | stage: {stage} | payload: {payload}")
    # Aqui você pode chamar Spark ou outro processador

def main():
    pubsub = r.pubsub()
    pubsub.subscribe(PUBSUB_CHANNEL)
    print("Aguardando notificações...")

    for msg in pubsub.listen():
        if msg['type'] == 'message':
            try:
                data = json.loads(msg['data'].decode())
                message_ids = data.get("message_ids", [])
                print(f"Notificação recebida para {len(message_ids)} mensagens")

                # Processa em blocos de 100
                for i in range(0, len(message_ids), 100):
                    batch_ids = message_ids[i:i + 100]
                    print(f"Processando bloco de {len(batch_ids)} mensagens")

                    for msg_id in batch_ids:
                        # Lê a mensagem pelo ID
                        msgs = r.xrange(STREAM_KEY, min=msg_id, max=msg_id)
                        for _, message in msgs:
                            process_message(msg_id, message)

            except Exception as e:
                print(f"Erro ao processar notificação: {e}")

if __name__ == "__main__":
    main()
