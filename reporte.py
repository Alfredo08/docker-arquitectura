"""Trabajo batch ejecutado por Spark dentro de Docker."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Ventas de la tienda").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    # Cada registro del archivo proviene de un mensaje del topic de Kafka.
    eventos = (
        spark.read.option("mode", "FAILFAST")
        .schema("venta_id STRING, ocurrio STRING, llego STRING, cantidad INT, precio INT")
        .csv("hdfs://namenode:8020/laboratorio/ventas.csv")
    )

    def reporte(filas, nombre):
        # La misma venta reintentada conserva id, cantidad y precio en este ejemplo.
        ventas = (filas.where("cantidad > 0 AND precio > 0")
                  .select("venta_id", "cantidad", "precio").distinct())
        print(nombre)
        ventas.agg(
            F.count("*").alias("ventas_validas"),
            F.sum(F.col("cantidad") * F.col("precio")).alias("total"),
        ).show()

    print("Eventos recibidos en HDFS:", eventos.count())
    reporte(eventos.where(F.col("llego") <= "09:07"), "Corte 09:07")
    reporte(eventos, "Todas las llegadas")
finally:
    spark.stop()
