import os
import sys

# 1. Variables de entorno de Java y Hadoop
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17" 
#os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["HADOOP_HOME"] + "\\bin;" + os.environ["PATH"]



os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 3. Rutas Absolutas Dinámicas
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR) 

TRAIN_PATH = os.path.join(BASE_DIR, "data", "raw", "train.csv")
TEST_PATH = os.path.join(BASE_DIR, "data", "raw", "test.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed", "amazon_reviews.parquet")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when

def process_batch_data():
    print("Iniciando Spark Session (Modo Seguro para Windows CMD)...")
    spark = SparkSession.builder \
        .appName("AmazonBigData_Batch") \
        .master("local[2]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()

    print(f"1. Leyendo datos de: {TRAIN_PATH}")
    df_train = spark.read.option("header", "true") \
                         .option("inferSchema", "true") \
                         .option("multiLine", "true") \
                         .option("escape", '"') \
                         .csv(TRAIN_PATH)
                         
    print(f"Leyendo datos de: {TEST_PATH}")
    df_test = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .option("multiLine", "true") \
                        .option("escape", '"') \
                        .csv(TEST_PATH)

    print("2. Ejecutando UNION (Transformación)...")
    df_full = df_train.union(df_test)

    print("3. Generando nuevas columnas (Feature Generation)...")
    df_refined = df_full.dropna() \
                        .withColumn("word_count", length(col("content")) / 5) \
                        .withColumn("sentiment_name", when(col("label") == 1, "Negative")
                                                      .when(col("label") == 2, "Positive")
                                                      .otherwise("Neutral"))

    print("Contando registros (Esto puede tardar un par de minutos, paciencia)...")
    total_rows = df_refined.count()
    print(f"Total de registros procesados: {total_rows}")

    print(f"4. Guardando en capa PROCESSED...\nRuta: {OUTPUT_PATH}")
    
    # NUEVO: repartition(4) divide el trabajo en 4 pedazos pequeños para no ahogar la RAM
    df_refined.repartition(4).write \
              .mode("overwrite") \
              .parquet(OUTPUT_PATH)
              
    print("¡Éxito! Proceso Batch terminado y guardado correctamente.")

    spark.stop()

if __name__ == "__main__":
    process_batch_data()