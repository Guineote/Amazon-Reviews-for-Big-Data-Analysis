import os
import sys

# 1. Variables de entorno
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17" 
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["HADOOP_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, round

# 2. Rutas dinámicas apuntando al PARQUET
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR) 
PARQUET_PATH = os.path.join(BASE_DIR, "data", "processed", "amazon_reviews.parquet")

def run_aggregations():
    print("Iniciando Spark Session para Analítica...")
    spark = SparkSession.builder \
        .appName("Amazon_Analytics") \
        .master("local[2]") \
        .getOrCreate()

    print("1. Leyendo capa Processed (Parquet)...")
    df_parquet = spark.read.parquet(PARQUET_PATH)

    print("2. Ejecutando JOIN...")
    # Creamos un pequeño catálogo en memoria para cruzarlo con nuestros datos
    catalog_data = [
        ("Positive", "High Priority - Retain"), 
        ("Negative", "Urgent - Contact Customer"), 
        ("Neutral", "Low Priority - Monitor")
    ]
    catalog_df = spark.createDataFrame(catalog_data, ["sentiment_name", "business_action"])
    
    # Hacemos el Join relacionando la columna "sentiment_name"
    df_joined = df_parquet.join(catalog_df, on="sentiment_name", how="left")

    print("3. Ejecutando AGGREGATIONS (Agrupación matemática)...")
    # Agrupamos por sentimiento para contar cuántas reseñas hay y el promedio de palabras
    df_summary = df_joined.groupBy("sentiment_name", "business_action").agg(
        count("*").alias("total_reviews"),
        round(avg("word_count"), 2).alias("average_words_per_review")
    )

    print("=== RESUMEN ANALÍTICO ===")
    # show() imprime el resultado en formato de tabla en la consola
    df_summary.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    run_aggregations()