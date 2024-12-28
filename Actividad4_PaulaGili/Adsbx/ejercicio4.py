import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import math


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


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance


haversine_udf = udf(haversine, DoubleType())


barcelona = (41.9774, 2.0800)
tarragona = (41.1184, 1.2500)
girona = (41.9028, 2.8214)


def add_distance(df, airport_name, lat1, lon1):
   
    distance_column = airport_name + "_distance"
    return df.withColumn(
        distance_column,
        haversine_udf(col("Latitud"), col("Longitud"), lit(lat1), lit(lon1))
    )

# Leer los datos del socket
flujo = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 21002) \
    .load()


json_flujo = flujo.select(from_json(col("value"), schema).alias("parsed_json"))
exploded = json_flujo.select(explode(col("parsed_json.ac")).alias("avion"))


aviones = exploded.select(
    col("avion.flight").alias("Vuelo"),
    col("avion.lat").alias("Latitud"),
    col("avion.lon").alias("Longitud"),
    col("avion.alt_baro").alias("Altitud"),
    col("avion.category").alias("Categoría")
)


aviones_con_distancia = aviones.filter(col("Categoría").isNotNull())


aviones_con_distancia = add_distance(aviones_con_distancia, "Barcelona", *barcelona)
aviones_con_distancia = add_distance(aviones_con_distancia, "Tarragona", *tarragona)
aviones_con_distancia = add_distance(aviones_con_distancia, "Girona", *girona)


query1 = aviones_con_distancia.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


categoria_count = aviones_con_distancia.groupBy("Categoría").count()

query2 = categoria_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query1.awaitTermination()
query2.awaitTermination()