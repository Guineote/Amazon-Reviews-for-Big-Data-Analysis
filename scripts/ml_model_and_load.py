import os
import sys

# 1. Variables de entorno (Nuestro confiable blindaje)
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17" 
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["HADOOP_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR) 
PARQUET_PATH = os.path.join(BASE_DIR, "data", "processed", "amazon_reviews.parquet")

def run_ml_and_load():
    print("Iniciando Spark con MLlib y conector PostgreSQL...")
    spark = SparkSession.builder \
        .appName("Amazon_ML_and_DB") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()

    print("1. Leyendo datos limpios...")
    # Tomamos solo un 5% de los datos para que el modelo entrene rápido en tu compu local
    df = spark.read.parquet(PARQUET_PATH).sample(fraction=0.05, seed=42)

    print("2. [PUNTO 9] Entrenando IA: Modelo de Clustering (K-Means)...")
    # Preparamos las columnas que la IA va a analizar
    assembler = VectorAssembler(inputCols=["label", "word_count"], outputCol="features")
    df_features = assembler.transform(df.na.drop(subset=["word_count"]))

    # Entrenamos a la IA para que busque 3 perfiles de clientes
    kmeans = KMeans(k=3, seed=1)
    model = kmeans.fit(df_features)
    
    # Hacemos las predicciones (asignar cada reseña a un cluster)
    predictions = model.transform(df_features)
    df_analytical = predictions.select("label", "sentiment_name", "word_count", "prediction") \
                               .withColumnRenamed("prediction", "customer_profile_cluster")

    evaluator = ClusteringEvaluator(predictionCol="prediction", featuresCol="features", metricName="silhouette")
    silhouette = evaluator.evaluate(predictions)
    
    print("="*50)
    print(f"MÉTRICA DE EVALUACIÓN DEL MODELO IA")
    print(f"Silhouette Score: {silhouette}")
    print("="*50)
    
    print("3. [Punto 7] Guardando Modelo Analítico en PostgreSQL...")
    # ====== CAMBIA ESTOS DATOS POR LOS TUYOS ======
    DB_URL = "jdbc:postgresql://localhost:5432/amazon_project"
    DB_PROPERTIES = {
        "user": "postgres", # Tu usuario de pgAdmin
        "password": "quixo284650", # Tu contraseña de pgAdmin
        "driver": "org.postgresql.Driver"
    }
    
    # Guardamos la tabla analítica estructurada en la BD
    df_analytical.write.jdbc(url=DB_URL, table="amazon_analytical_model", mode="overwrite", properties=DB_PROPERTIES)

    print("IA entrenada y datos estructurados guardados en PostgreSQL.")
    spark.stop()

if __name__ == "__main__":
    run_ml_and_load()