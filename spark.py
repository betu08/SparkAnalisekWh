import os
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17"
os.environ["SPARK_HOME"] = "C:/Spark/spark-3.5.2-bin-hadoop3/spark-3.5.2-bin-hadoop3"

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('spark').getOrCreate()

from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql import functions as f

# Acessando o arquivo csv
csv_file_path = "C:/Users/rober/PycharmProjects/pythonProject/data/arquivo.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Realizando o tratamento de dados da planilha
df = df.withColumn('Valor_Total', f.regexp_replace('Valor_Total',',','.'))
df = df.withColumn('Geladeira', f.regexp_replace('Geladeira',',','.'))
df = df.withColumn('Maquina_de_lavar', f.regexp_replace('Maquina_de_lavar',',','.'))
df = df.withColumn('TV', f.regexp_replace('TV',',','.'))
df = df.withColumn('Ar_condicionado', f.regexp_replace('Ar_condicionado',',','.'))

df = df.withColumn('Valor_Total', df['Valor_Total'].cast(DoubleType()))
df = df.withColumn('kWh', df['kWh'].cast(DoubleType()))
df = df.withColumn('Geladeira', df['Geladeira'].cast(DoubleType()))
df = df.withColumn('Maquina_de_lavar', df['Maquina_de_lavar'].cast(DoubleType()))
df = df.withColumn('TV', df['TV'].cast(DoubleType()))
df = df.withColumn('Ar_condicionado', df['Ar_condicionado'].cast(DoubleType()))

# Criando uma view temporaria para consulta sql
df.createOrReplaceTempView("ELETRO_KWH")
meses_df = spark.sql("SELECT Mes FROM ELETRO_KWH")

# Coletar os meses em uma lista
lista_meses = [row.Mes for row in meses_df.collect()]

for mes in lista_meses:
    query = f"""
        SELECT Mes ,
            kWh / Valor_Total AS Valor_kWh_Mes,
            kWh / Valor_Total * Geladeira AS Valor_Hora_Geladeira,
            kWh / Valor_Total * Maquina_de_lavar AS Valor_Hora_Maquina_de_lavar,
            kWh / Valor_Total * TV AS Valor_Hora_TV,
            kWh / Valor_Total * Ar_condicionado AS Valor_Hora_Ar_condicionado
        FROM 
            ELETRO_KWH 
        WHERE 
            Mes = '{mes}'
    """
    spark.sql(query).show()

#Mostrando o esquema do DataFrame
df.printSchema()
spark.stop()