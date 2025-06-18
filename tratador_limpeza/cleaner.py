import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data-cleaner')

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_limpeza_group"
SOURCE_TOPIC = "raw_secretary"
DEST_TOPIC = "clean_secretary"

def wait_for_kafka(max_retries=30, sleep_time=5):
    """Aguarda o Kafka ficar disponível"""
    from confluent_kafka import KafkaException
    retry_count = 0
    while retry_count < max_retries:
        try:
            test_consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "test_connection",
                "auto.offset.reset": "earliest"
            })
            test_consumer.subscribe(["test_topic"])
            test_consumer.close()
            logger.info("Kafka está disponível")
            return True
        except KafkaException as e:
            logger.warning(f"Tentativa {retry_count + 1}/{max_retries} - Kafka não disponível: {e}")
            time.sleep(sleep_time)
            retry_count += 1
    raise Exception("Não foi possível conectar ao Kafka após várias tentativas")

def limpeza(dado):
    """Função de limpeza com logs detalhados"""
    logger.info(f"Iniciando limpeza para registro: {dado}")
    
    if not isinstance(dado, dict):
        logger.error("Dado inválido: não é um dicionário")
        return None
    
    # Validação do campo Diagnostico
    if dado.get("Diagnostico") not in [0, 1]:
        logger.error(f"Registro descartado - Diagnóstico inválido: {dado.get('Diagnostico')}")
        return None
    
    # Validação do campo Populacao
    if not isinstance(dado.get("Populacao"), int) or dado.get("Populacao", 0) <= 0:
        logger.error(f"Registro descartado - População inválida: {dado.get('Populacao')}")
        return None
    
    logger.info("Registro validado com sucesso")
    return dado

def create_kafka_consumer():
    """Cria e configura o consumidor Kafka"""
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 60000,
        "heartbeat.interval.ms": 20000
    }
    return Consumer(consumer_config)

def create_kafka_producer():
    """Cria e configura o produtor Kafka"""
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "message.timeout.ms": 5000,
        "retries": 5
    }
    return Producer(producer_config)

def main():
    logger.info("\n" + "="*50)
    logger.info(" INICIANDO TRATADOR DE LIMPEZA DE DADOS ")
    logger.info("="*50)
    
    # Aguardar Kafka ficar disponível
    wait_for_kafka()

    consumer = create_kafka_consumer()
    producer = create_kafka_producer()

    def delivery_report(err, msg):
        """Callback para confirmação de entrega"""
        if err is not None:
            logger.error(f"Falha ao enviar mensagem: {err}")
        else:
            logger.info(f"Mensagem enviada para {msg.topic()} [partição {msg.partition()}]")

    consumer.subscribe([SOURCE_TOPIC])
    logger.info(f"Inscrito no tópico {SOURCE_TOPIC}. Aguardando mensagens...")

    try:
        msg_count = 0
        start_time = time.time()
        
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Fim da partição alcançado")
                    continue
                else:
                    logger.error(f"Erro no consumidor Kafka: {msg.error()}")
                    continue

            msg_count += 1
            logger.info(f"Mensagem #{msg_count} recebida [tópico: {msg.topic()}, partição: {msg.partition()}, offset: {msg.offset()}]")
            
            try:
                # Processamento da mensagem
                dado = json.loads(msg.value().decode('utf-8'))
                logger.debug(f"Conteúdo bruto: {dado}")
                
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
                    logger.debug(f"Dado limpo: {dado_limpo}")
                else:
                    logger.info("Registro descartado durante a limpeza")
                    
                # Commit do offset
                consumer.commit(asynchronous=False)
                logger.debug(f"Offset {msg.offset()} confirmado")
                
            except json.JSONDecodeError as e:
                logger.error(f"Erro ao decodificar JSON: {e}")
            except Exception as e:
                logger.error(f"Erro inesperado: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("\n" + "="*50)
        logger.info(" INTERRUPÇÃO SOLICITADA - ENCERRANDO CONSUMER ")
        logger.info(f"Total de mensagens processadas: {msg_count}")
        logger.info(f"Tempo de execução: {time.time() - start_time:.2f} segundos")
        logger.info("="*50)
    finally:
        consumer.close()
        logger.info("Conexão com Kafka encerrada corretamente")

if __name__ == "__main__":
    main()