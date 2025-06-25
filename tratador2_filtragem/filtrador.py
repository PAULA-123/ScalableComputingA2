from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SOURCE_TOPIC = "clean_secretary"
DEST_TOPIC = "filtered_secretary"

# Schema do JSON esperado
schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def main():
    spark = SparkSession.builder.appName("tratador_filtragem_streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Lê o stream do Kafka
    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", SOURCE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Converte o valor binário para string e parseia o JSON com o schema
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json("json_str", schema).alias("data")) \
        .select("data.*")

    # Aplica o filtro diretamente em Spark DataFrame
    df_filtered = df_parsed.filter(
        (col("Populacao") > 1000) &
        (col("Diagnostico").isNotNull()) &
        (col("CEP").isNotNull()) &
        ((col("Vacinado") == 1) | (col("Diagnostico") == 1))
    )

    # Converte novamente para JSON para enviar ao Kafka
    df_to_kafka = df_filtered.select(to_json(struct("*")).alias("value"))

    # Escreve o stream no Kafka no tópico de destino
    query = df_to_kafka.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", DEST_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint_tratador_filtragem") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
