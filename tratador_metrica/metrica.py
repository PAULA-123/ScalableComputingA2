import json
from statistics import mean
from confluent_kafka import Consumer, KafkaError
import os

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_metricas_group"
SOURCE_TOPIC = "filtered_secretary"
OUTPUT_FILE = "databases_mock/resultados_metrica.json"

def salvar_resultado(resultados):
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(resultados, f, indent=4)
    except Exception as e:
        print(f"[ERRO] Falha ao salvar arquivo JSON: {e}")

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([SOURCE_TOPIC])
    valores = []
    resultados = []

    print(f"[METRICA] Escutando tópico: {SOURCE_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[METRICA] Erro no Kafka: {msg.error()}")
                    continue
            try:
                dado = json.loads(msg.value().decode('utf-8'))
                if "Diagnostico" in dado:
                    valores.append(dado["Diagnostico"])

                    if len(valores) % 10 == 0:
                        media = mean(valores)
                        resultado = {
                            "quantidade": len(valores),
                            "media_diagnostico": round(media, 4)
                        }
                        resultados.append(resultado)
                        print(f"[METRICA] Resultado #{len(resultados)}:", resultado)

                        salvar_resultado(resultados)
            except Exception as e:
                print(f"[METRICA] Erro ao processar mensagem: {e}")
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
