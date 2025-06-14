from pyspark.sql import SparkSession

# Cria sessão Spark
spark = SparkSession.builder.appName("ExemploSimples").getOrCreate()

# Cria DataFrame de exemplo
data = [("Ana", 20), ("Bruno", 30), ("Carla", 25)]
df = spark.createDataFrame(data, ["Nome", "Idade"])

# Operação simples
df_filtered = df.filter(df["Idade"] > 21)
df_filtered.show()

spark.stop()
