import json
import time
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_limpeza_group"
SOURCE_TOPIC = "raw_secretary"
DEST_TOPIC = "clean_secretary"

def limpeza(dado):
    """Função de limpeza com logs detalhados"""
    print(f"\n--- INICIANDO LIMPEZA PARA REGISTRO ---")
    print(f"Dado recebido: {dado}")
    
    # Validação do campo Diagnostico
    if dado.get("Diagnostico") not in [0, 1]:
        print(f"🚫 Registro descartado - Diagnóstico inválido: {dado.get('Diagnostico')}")
        return None
    
    # Validação do campo Populacao
    if not isinstance(dado.get("Populacao"), int) or dado.get("Populacao", 0) <= 0:
        print(f"🚫 Registro descartado - População inválida: {dado.get('Populacao')}")
        return None
    
    print("✅ Registro validado com sucesso")
    return dado

def main():
    print("\n" + "="*50)
    print(" INICIANDO TRATADOR DE LIMPEZA DE DADOS ")
    print("="*50)
    print(f"Conectando ao Kafka em: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Tópico de origem: {SOURCE_TOPIC}")
    print(f"Tópico de destino: {DEST_TOPIC}")
    print(f"Grupo de consumidores: {GROUP_ID}")
    print("="*50 + "\n")

    # Configuração do consumidor
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }

    consumer = Consumer(consumer_config)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    def delivery_report(err, msg):
        """Callback para confirmação de entrega"""
        if err is not None:
            print(f"❌ Falha ao enviar mensagem: {err}")
        else:
            print(f"📤 Mensagem enviada com sucesso para {msg.topic()} [partição {msg.partition()}]")

    consumer.subscribe([SOURCE_TOPIC])
    print(f"🔍 Inscrito no tópico {SOURCE_TOPIC}. Aguardando mensagens...")

    try:
        msg_count = 0
        start_time = time.time()
        
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("ℹ️ Fim da partição alcançado")
                    continue
                else:
                    print(f"❌ Erro no consumidor Kafka: {msg.error()}")
                    continue

            msg_count += 1
            print(f"\n📥 Mensagem #{msg_count} recebida [tópico: {msg.topic()}, partição: {msg.partition()}, offset: {msg.offset()}]")
            
            try:
                # Processamento da mensagem
                dado = json.loads(msg.value().decode('utf-8'))
                print(f"📝 Conteúdo bruto: {dado}")
                
                # Limpeza dos dados
                dado_limpo = limpeza(dado)
                
                if dado_limpo:
                    # Envio para o tópico de saída
                    producer.produce(
                        DEST_TOPIC,
                        json.dumps(dado_limpo).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.flush()
                    print(f"🔄 Dado limpo: {dado_limpo}")
                else:
                    print("🗑️ Registro descartado durante a limpeza")
                    
                # Commit do offset
                consumer.commit(asynchronous=False)
                print(f"✔️ Offset {msg.offset()} confirmado")
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON: {e}")
            except Exception as e:
                print(f"❌ Erro inesperado: {e}")

    except KeyboardInterrupt:
        print("\n" + "="*50)
        print(" INTERRUPÇÃO SOLICITADA - ENCERRANDO CONSUMER ")
        print(f"Total de mensagens processadas: {msg_count}")
        print(f"Tempo de execução: {time.time() - start_time:.2f} segundos")
        print("="*50)
    finally:
        consumer.close()
        print("✅ Conexão com Kafka encerrada corretamente")

if __name__ == "__main__":
    main()