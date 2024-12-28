# KPI del calculo de la altitud promedio de los vuelos en Cataluña

# Importamos las librerías necesarias
import findspark
findspark.init()  

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, explode, avg
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
spark = SparkSession.builder.appName("KPI: Altitud Promedio de Vuelos").getOrCreate()

# Puerto para recibir datos desde el socket
PORT = 21002

# Leemos el flujo de datos desde el socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", PORT) \
    .load()

# Convertimos los datos en formato JSON y aplicamos el esquema
json_flujo = lines.select(from_json(col("value"), schema).alias("parsed_json"))

# Extraemos la columna 'now' y la convertimos en un timestamp
json_flujo = json_flujo.withColumn("timestamp", from_unixtime(col("parsed_json.now") / 1000).cast("timestamp"))

# Extraemos y explotamos el array 'ac' para obtener filas separadas para cada avión
exploded_data = json_flujo.select(explode(col("parsed_json.ac")).alias("flight_data"), "timestamp")

# Filtramos los vuelos en Cataluña (coordenadas aproximadas de Cataluña)
filtered_data = exploded_data.filter(
    (col("flight_data.lat") >= 40.294028) & (col("flight_data.lat") <= 42.924299) &
    (col("flight_data.lon") >= 0.500251) & (col("flight_data.lon") <= 3.567923)
)

# Calculamos la altitud promedio de los vuelos en Cataluña
avg_altitude = filtered_data.select(avg(col("flight_data.alt_baro")).alias("avg_altitude"))

# Escribimos la salida a la consola en tiempo real
query = avg_altitude.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Mantenemos el stream por detención manual
query.awaitTermination()




# 1. Nombre del KPI:
# Altitud Promedio de los Vuelos en Cataluña

# 2. Definición del KPI:
# Este KPI mide la altitud promedio de los vuelos que pasan por la región de Cataluña en tiempo real. Se calcula a partir de los datos de los vuelos, específicamente tomando la altitud barométrica (alt_baro) de cada vuelo que pasa por las coordenadas geográficas de Cataluña (latitudes y longitudes definidas). La altitud promedio se calcula de forma continua para todos los vuelos que están en esa área.

# 3. Fórmula de Cálculo:
# La fórmula para calcular la altitud promedio es:

# Altitud Promedio = Σ alt_baro / N

# Donde:
# - alt_baro es la altitud barométrica de cada vuelo.
# - N es el número total de vuelos registrados en la región de Cataluña durante un periodo de tiempo específico.
# En este caso, el cálculo es realizado en tiempo real a través de un flujo de datos.

# 4. Datos a Usar:
# - Latitud y longitud de los vuelos, con el fin de filtrar los vuelos que se encuentran dentro de los límites geográficos de Cataluña.
# - Altitud barométrica (alt_baro) de cada vuelo, que es el dato relevante para este KPI.
# - Timestamp de los vuelos, utilizado para la recolección y medición del flujo de datos.

# 5. Unidades de Medida:
# - Altitud barométrica: Metros (m)
# - Frecuencia de medición: Este KPI se mide en tiempo real, con una frecuencia de actualización continua basada en los flujos de datos entrantes desde el socket.

# 6. Frecuencia de Medición:
# La frecuencia de medición es en tiempo real y depende de la llegada de nuevos datos de vuelos que pasan por las coordenadas de Cataluña. Es un flujo continuo que se actualiza constantemente.
