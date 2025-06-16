from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnan, lit
from pyspark.sql.types import NumericType, StringType, BooleanType
from pyspark.ml.feature import Imputer

class NullHandler:
    """
    Classe para tratamento de valores nulos em DataFrames do PySpark.
    Projetada para trabalhar em conjunto com um extrator de dados.
    
    Parâmetros:
    - df: DataFrame do PySpark já carregado
    - config: Dicionário com configurações personalizadas (opcional)
    """
    
    def __init__(self, df, config=None):
        self.df = df
        self.config = config or {
            'column_threshold': 60.0,  # % máximo de nulos para manter coluna
            'row_threshold': 3,        # máx de nulos por linha para mantê-la
            'numeric_strategy': 'mean',  # mean, median ou mode
            'default_string': 'UNKNOWN',
            'default_boolean': False,
            'custom_imputations': {}  # sobrescreve padrões por coluna
        }
        
        # Validação inicial
        if not isinstance(self.df, type(SparkSession.builder.getOrCreate().createDataFrame([], 'dummy STRING'))):
            raise ValueError("O parâmetro df deve ser um DataFrame do PySpark")
    
    def analyze_nulls(self):
        """Analisa e retorna estatísticas de valores nulos"""
        total_rows = self.df.count()
        null_stats = []
        
        for column in self.df.columns:
            null_count = self.df.where(col(column).isNull() | isnan(col(column))).count()
            null_percent = (null_count / total_rows) * 100 if total_rows > 0 else 0
            dtype = str(self.df.schema[column].dataType)
            null_stats.append({
                'column': column,
                'null_count': null_count,
                'null_percent': null_percent,
                'type': dtype
            })
        
        return null_stats
    
    def _get_imputation_value(self, column, dtype):
        """Determina o valor de imputação baseado no tipo e configurações"""
        # Verifica se há valor customizado
        if column in self.config['custom_imputations']:
            return self.config['custom_imputations'][column]
        
        # Valores padrão por tipo
        if isinstance(dtype, NumericType):
            # Será tratado pelo Imputer mais tarde
            return None
        elif isinstance(dtype, StringType):
            return self.config['default_string']
        elif isinstance(dtype, BooleanType):
            return self.config['default_boolean']
        return None
    
    def drop_high_null_columns(self):
        """Remove colunas com porcentagem de nulos acima do threshold"""
        null_stats = self.analyze_nulls()
        cols_to_drop = [
            stat['column'] 
            for stat in null_stats 
            if stat['null_percent'] > self.config['column_threshold']
        ]
        
        if cols_to_drop:
            print(f"Removendo colunas com >{self.config['column_threshold']}% nulos: {cols_to_drop}")
            self.df = self.df.drop(*cols_to_drop)
        
        return self
    
    def impute_nulls(self):
        """Preenche valores nulos conforme estratégias configuradas"""
        # Separa colunas por tipo
        numeric_cols = []
        other_cols = []
        
        for field in self.df.schema.fields:
            if isinstance(field.dataType, NumericType):
                numeric_cols.append(field.name)
            else:
                other_cols.append(field.name)
        
        # Imputação para colunas numéricas
        if numeric_cols and self.config['numeric_strategy']:
            imputer = Imputer(
                inputCols=numeric_cols,
                outputCols=[f"{c}_imputed" for c in numeric_cols],
                strategy=self.config['numeric_strategy']
            )
            model = imputer.fit(self.df)
            self.df = model.transform(self.df)
            
            # Remove originais e renomeia as imputadas
            for col_name in numeric_cols:
                self.df = self.df.drop(col_name).withColumnRenamed(f"{col_name}_imputed", col_name)
        
        # Imputação para outras colunas
        for col_name in other_cols:
            dtype = self.df.schema[col_name].dataType
            fill_value = self._get_imputation_value(col_name, dtype)
            
            if fill_value is not None:
                self.df = self.df.withColumn(
                    col_name,
                    when(col(col_name).isNull() | isnan(col(col_name)), fill_value).otherwise(col(col_name)))
        
        return self
    
    def drop_high_null_rows(self):
        """Remove linhas com muitos valores nulos"""
        # Conta nulos por linha
        null_counts = [count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in self.df.columns]
        row_counts = self.df.select(*null_counts)
        
        # Calcula total de nulos por linha
        total_nulls = sum(col(c) for c in self.df.columns)
        row_counts = row_counts.withColumn("null_count", total_nulls)
        
        # Filtra linhas
        initial_count = self.df.count()
        self.df = self.df.join(row_counts.select("null_count"), how='left')
        self.df = self.df.filter(col("null_count") <= self.config['row_threshold']).drop("null_count")
        final_count = self.df.count()
        
        print(f"Removidas {initial_count - final_count} linhas com >{self.config['row_threshold']} valores nulos")
        return self
    
    def get_processed_df(self):
        """Retorna o DataFrame após todas as transformações"""
        return self.df
    
    def full_process(self):
        """Executa o pipeline completo de tratamento"""
        print("\n=== Análise Inicial de Nulos ===")
        for stat in self.analyze_nulls():
            print(f"Coluna: {stat['column']:15} | Tipo: {stat['type']:10} | Nulos: {stat['null_count']:6} ({stat['null_percent']:5.2f}%)")
        
        self.drop_high_null_columns()
        self.drop_high_null_rows()
        self.impute_nulls()
        
        print("\n=== Resultado Final ===")
        for stat in self.analyze_nulls():
            print(f"Coluna: {stat['column']:15} | Nulos: {stat['null_count']:6} ({stat['null_percent']:5.2f}%)")
        
        return self.df


# Exemplo de uso integrado com um extrator
if __name__ == "__main__":
    # Simulando um cenário onde o DataFrame vem de um extrator
    spark = SparkSession.builder.appName("NullHandlerExample").getOrCreate()
    
    # Exemplo de DataFrame (substituir pelo seu extrator quando estiver pronto)
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
    
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("vaccinated", BooleanType()),
        StructField("score", IntegerType()),
        StructField("department", StringType())
    ])
    
    data = [
        (1, "Alice", 34, True, 85, "HR"),
        (2, None, 45, None, None, None),
        (3, "Bob", None, False, 90, "IT"),
        (4, None, None, None, None, None),
        (5, "Eve", 28, True, None, "Finance")
    ]
    
    extracted_df = spark.createDataFrame(data, schema)
    
    # Configuração personalizada (opcional)
    config = {
        'column_threshold': 50.0,
        'row_threshold': 2,
        'numeric_strategy': 'mean',
        'default_string': 'MISSING',
        'default_boolean': False,
        'custom_imputations': {
            'age': 30,  # Preenche idade nula com 30
            'department': 'UNASSIGNED'
        }
    }
    
    # Uso do NullHandler
    print("=== DataFrame Original ===")
    extracted_df.show()
    
    null_handler = NullHandler(extracted_df, config)
    processed_df = null_handler.full_process()
    
    print("\n=== DataFrame Processado ===")
    processed_df.show()
    
    spark.stop()