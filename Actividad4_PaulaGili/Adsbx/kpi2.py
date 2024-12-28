#KPI: media de vuelos por hora.

# Importamos las librerías necesarias
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, window, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql import functions as F

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
spark = SparkSession.builder.appName("KPI: Media de Vuelos por Hora").getOrCreate()

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

# Contamos el número de vuelos por ventana de tiempo de 1 hora
flights_per_hour = exploded_data \
    .groupBy(window(col("timestamp"), "1 hour")) \
    .agg(F.count("flight_data.flight").alias("num_flights"))

# En lugar de aplicar la agregación en la misma operación de streaming, ahora la hacemos en un paso posterior
avg_flights_per_hour = flights_per_hour \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Mantenemos el stream por detención manual
avg_flights_per_hour.awaitTermination()




#1. Nombre del KPI:
#Media de Vuelos por Hora

#2. Definición del KPI:
#Este KPI mide la cantidad promedio de vuelos que pasan por la región de interés durante un intervalo de una hora. Se calcula a partir de los datos de los vuelos, específicamente utilizando la marca de tiempo (timestamp) de cada vuelo y agrupando los vuelos en ventanas de una hora. Este cálculo se actualiza en tiempo real con los flujos de datos.

#3. Fórmula de Cálculo:

#Media de Vuelos por Hora = N / T

#Donde:
#- N es el número total de vuelos que han pasado por la región en la ventana de tiempo de una hora.
#- T es el total de horas consideradas (en este caso, una hora).
#Este cálculo se realiza en tiempo real con los datos que llegan por streaming.

#4. Datos a Usar:
#- Timestamp de cada vuelo, para agrupar los vuelos en intervalos de una hora.
#- Información sobre cada vuelo, como la identificación de vuelo y las coordenadas, para contar el número de vuelos en cada ventana.

#5. Unidades de Medida:
#- Número de vuelos: Unidades enteras (número de vuelos).
#- Frecuencia de medición: Este KPI se mide cada vez que se actualiza el flujo de datos, es decir, en intervalos de tiempo de una hora.

#6. Frecuencia de Medición:
#La frecuencia de medición es en tiempo real, con actualizaciones basadas en los flujos de datos entrantes. La agregación se hace cada hora a medida que los datos de los vuelos se procesan en tiempo real.
