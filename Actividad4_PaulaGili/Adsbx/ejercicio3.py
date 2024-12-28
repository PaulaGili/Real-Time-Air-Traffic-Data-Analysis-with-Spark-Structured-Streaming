import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Definimos el esquema JSON
schema = StructType([
    StructField("ac", ArrayType(StructType([
        StructField("flight", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("alt_baro", DoubleType(), True),
        StructField("category", StringType(), True)
    ])), True),
    StructField("ctime", DoubleType(), True),
    StructField("msg", StringType(), True),
    StructField("now", DoubleType(), True),
    StructField("ptime", DoubleType(), True),
    StructField("total", DoubleType(), True)
])

# Iniciamos sesión de Spark
spark = SparkSession.builder.appName("Structured Streaming Pre-procesado").getOrCreate()

# Puerto
PORT = 21002

# Se lee el flujo de datos con el socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", PORT) \
    .load()

# Pasamos la columna 'value' a JSON y se aplicar el esquema
json_flujo = lines.select(from_json(col("value"), schema).alias("parsed_json"))

# Extraemos la columna 'now' y explotamos 'ac' para convertirla a timestamp
json_flujo = json_flujo.withColumn("timestamp", from_unixtime(col("parsed_json.now") / 1000).cast("timestamp"))

# Extraemos el array 'ac' y explotamos para que sean filas separadas
exploded_data = json_flujo.select(explode(col("parsed_json.ac")).alias("flight_data"), "timestamp")

# Filtramos los vuelos en Cataluña
filtered_data = exploded_data.filter(
    (col("flight_data.lat") >= 40.294028) & (col("flight_data.lat") <= 42.924299) &
    (col("flight_data.lon") >= 0.500251) & (col("flight_data.lon") <= 3.567923)
)

# Seleccionamos las columnas de interés con timestamp
final_data = filtered_data.select(
    "flight_data.flight",
    "flight_data.lat",
    "flight_data.lon",
    "flight_data.alt_baro",
    "flight_data.category",
    "timestamp"
)

# Mostramos los datos que hemos filtrado con la columna 'timestamp' en la consola
query = final_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Mantenemos el stream por detencion manual
query.awaitTermination()