import json
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_filtro_group"
SOURCE_TOPIC = "clean_secretary"
DEST_TOPIC = "filtered_secretary"

def filtrar(dado):
    # Só aceita CEP entre 11000 e 30999 e Diagnostico não nulo
    try:
        cep = int(dado.get("CEP", 0))
    except Exception:
        return None
    if 11000 <= cep <= 30999 and dado.get("Diagnostico") is not None:
        return dado
    return None

def main():
    print("Conectando ao Kafka...")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"Escutando tópico: {SOURCE_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Erro no Kafka: {msg.error()}")
                    continue
            try:
                dado = json.loads(msg.value().decode('utf-8'))
                dado_filtrado = filtrar(dado)
                if dado_filtrado:
                    producer.produce(DEST_TOPIC, json.dumps(dado_filtrado).encode('utf-8'))
                    producer.flush()
            except Exception as e:
                print(f"Erro ao processar mensagem: {e}")
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
