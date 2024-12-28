import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode,from_json,schema_of_json,lit
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

#Añade las importaciones que consideres necesarias

ejemplo='{"ac": [{"flight": "RYR80XN ","lat": 40.783493,"lon": -9.551697, "alt_baro": 37000,"category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'
encabezados = ['flight','lat','lon','alt_baro','category']  


spark = SparkSession.builder.appName("STRUCTURED STREAMING").getOrCreate()

schema = StructType([
    StructField("ac", ArrayType(StructType([  
        StructField("flight", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("alt_baro", DoubleType(), True),
        StructField("category", StringType(), True)
    ])), True),
    StructField("ctime", StringType(), True),
    StructField("msg", StringType(), True),
    StructField("now", StringType(), True),
    StructField("ptime", DoubleType(), True),
    StructField("total", DoubleType(), True)
])

flujo = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 21002) \
    .load()

#  Procesamiento: Interpretar JSON y obtener los campos relevantes
json_flujo = flujo.select(from_json(col("value"), schema).alias("parsed_json"))
exploded = json_flujo.select(explode(col("parsed_json.ac")).alias("avion"))

# Seleccionar columnas específicas
resultado = exploded.select(
    col("avion.flight").alias("Vuelo"),
    col("avion.lat").alias("Latitud"),
    col("avion.lon").alias("Longitud"),
    col("avion.alt_baro").alias("Altitud"),
    col("avion.category").alias("Categoría")
)

# Mostrar el resultado en consola
query = resultado.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()