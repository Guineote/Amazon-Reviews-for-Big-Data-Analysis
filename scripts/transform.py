import os
import sys
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17" 
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when

spark = SparkSession.builder \
    .appName("AmazonBigData") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

df_train = spark.read.csv("data/raw/train.csv", header=True, inferSchema=True)
df_test = spark.read.csv("data/raw/test.csv", header=True, inferSchema=True)

df_full = df_train.union(df_test)

df_refined = df_full.withColumn("word_count", length(col("content")) / 5) \
                    .withColumn("sentiment_name", when(col("label") == 1, "Negative").otherwise("Positive"))

print(f"Total de registros procesados: {df_refined.count()}")
df_refined.show(5)

def process_batch_data():
    print("Iniciando Spark Session...")
    spark = SparkSession.builder \
        .appName("AmazonBigData_Batch") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()

    print("1. Leyendo datos de la capa RAW...")
    df_train = spark.read.csv(r"data\01_raw\train.csv", header=True, inferSchema=True)
    df_test = spark.read.csv(r"data\01_raw\test.csv", header=True, inferSchema=True)

    print("2. Ejecutando UNION (Transformación)...")
    df_full = df_train.union(df_test)

    print("3. Generando nuevas columnas (Feature Generation)...")
    df_refined = df_full.dropna() \
                        .withColumn("word_count", length(col("content")) / 5) \
                        .withColumn("sentiment_name", when(col("label") == 1, "Negative")
                                                      .when(col("label") == 2, "Positive")
                                                      .otherwise("Neutral"))

    total_rows = df_refined.count()
    print(f"Total de registros procesados listos para guardar: {total_rows}")

    print("4. Guardando en capa PROCESSED (Formato Parquet)...")
    output_path = r"C:\Users\jared\Documents\Universidad\8vo semestre\Big Data\Amazon Big Data\data\processed"
    
    df_refined.write \
              .mode("overwrite") \
              .parquet(output_path)
              
    print(f"Datos guardados en: {output_path}")

    spark.stop()

if __name__ == "__main__":
    process_batch_data()

spark.stop()

