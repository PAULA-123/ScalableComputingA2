def limpeza(df):
    return df.dropDuplicates().na.drop(how='any')

def remover_colunas_vazias(df):
    return df.dropna(how='all')
