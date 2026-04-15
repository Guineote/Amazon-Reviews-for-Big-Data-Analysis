import os
import sys
import time
import threading
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Variables de entorno 
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17" 
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["HADOOP_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2. Rutas dinámicas
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR) 
STREAM_DIR = os.path.join(BASE_DIR, "data", "streaming")

# Creamos la carpeta de streaming si no existe
os.makedirs(STREAM_DIR, exist_ok=True)

# Limpiamos archivos
for f in os.listdir(STREAM_DIR):
    if f.endswith(".csv"):
        os.remove(os.path.join(STREAM_DIR, f))

def simulate_data_arrival():
    """HILO SECUNDARIO: Simula usuarios publicando reseñas en la web en tiempo real"""
    time.sleep(8) # Esperamos a que Spark encienda su radar
    print("\n[App Web] -> Usuario acaba de publicar 2 reseñas (Micro-lote 1)")
    with open(os.path.join(STREAM_DIR, "stream_1.csv"), "w", encoding="utf-8") as f:
        f.write("2,I absolutely love this product!\n2,Excellent quality for the price\n")
    
    time.sleep(10)
    print("\n[App Web] -> Usuario acaba de publicar 2 reseñas (Micro-lote 2)")
    with open(os.path.join(STREAM_DIR, "stream_2.csv"), "w", encoding="utf-8") as f:
        f.write("1,It arrived broken and useless\n2,Very useful item\n")

    time.sleep(10)
    print("\n[App Web] -> Usuario acaba de publicar 2 reseñas (Micro-lote 3)")
    with open(os.path.join(STREAM_DIR, "stream_3.csv"), "w", encoding="utf-8") as f:
        f.write("1,Worst customer service ever\n1,Do not buy this trash\n")

def run_streaming():
    """HILO PRINCIPAL: PySpark Structured Streaming leyendo micro-batches"""
    print("Iniciando Spark Structured Streaming...")
    # Quitamos los avisos molestos de la consola para que la captura salga limpia
    spark = SparkSession.builder \
        .appName("Amazon_MicroBatch_Streaming") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # En Streaming, Spark nos obliga a definir el esquema exacto de antemano
    schema = StructType([
        StructField("label", IntegerType(), True),
        StructField("content", StringType(), True)
    ])

    print(f"RADAR ACTIVADO: Escuchando nuevos eventos en la ruta:\n{STREAM_DIR}")
    
    # READ STREAM: Leemos de la carpeta procesando 1 archivo a la vez (Micro-Batch)
    streaming_df = spark.readStream \
        .format("csv") \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .load(STREAM_DIR)

    # TRANSFORMACIÓN INCREMENTAL: Conteo de sentimientos en tiempo real
    counts_df = streaming_df.groupBy("label").count()

    # WRITE STREAM: Escribimos el resultado directamente a la consola
    query = counts_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .start()

    # El script se mantendrá vivo escuchando por 40 segundos y luego se apagará solo
    query.awaitTermination(timeout=60)
    print("\n[Sistema] -> Simulación de Streaming finalizada con éxito.")
    spark.stop()

if __name__ == "__main__":
    # Arrancamos el simulador en un hilo paralelo
    threading.Thread(target=simulate_data_arrival, daemon=True).start()
    # Arrancamos Spark Streaming
    run_streaming()