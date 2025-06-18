from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder \
    .appName("TesteMetricaDiagnostico") \
    .getOrCreate()

df = spark.read.json("dados.json")
print("===== Dados carregados ======")
df.show()

resultado = df.agg(avg(col("Diagnostico")).alias("media_diagnostico"))
print("===== Resultado da média =====")
resultado.show()

spark.stop()
