from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SOURCE_TOPIC = "raw_secretary"
DEST_TOPIC = "clean_secretary"

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def limpeza(df):
    return df.filter(
        (col("Diagnostico").isin(0, 1)) &
        (col("Vacinado").isin(0, 1)) &
        (col("CEP").between(11001, 30999)) &
        (col("Escolaridade").between(0, 5)) &
        (col("Populacao") > 0) &
        (col("Data").isNotNull())
    )

spark = SparkSession.builder.appName("tratador_limpeza").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Ler do Kafka (streaming)
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# 2. Valor do Kafka vem em "value" como bytes, converter para string
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# 3. Parsear JSON para colunas usando schema
df_parsed = df_json.select(from_json("json_str", schema).alias("data")).select("data.*")

# 4. Aplicar limpeza
df_limpo = limpeza(df_parsed)

# 5. Converter para JSON para enviar ao Kafka
from pyspark.sql.functions import to_json, struct
df_out = df_limpo.select(to_json(struct([df_limpo[x] for x in df_limpo.columns])).alias("value"))

# 6. Gravar para Kafka (streaming sink)
query = (
    df_out.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", DEST_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints/tratador_limpeza")  # ajuste caminho
    .start()
)

query.awaitTermination()
