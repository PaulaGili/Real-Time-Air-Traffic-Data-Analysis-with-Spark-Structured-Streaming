import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType

# Ejemplo de cadena JSON para inferir el esquema
ejemplo = '{"ac": [{"flight": "RYR80XN ","lat": 40.783493,"lon": -9.551697, "alt_baro": 37000,"category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'

# 1. Se definine el esquema 
schema = StringType() 

# 2. Iniciamos la sesión en Spark
spark = SparkSession.builder.appName("STRUCTURED STREAMING Pre-procesado").getOrCreate()

# 3. Configuramos del puerto
PORT = 21002

# 4. Leemos el flujo de datos desde el socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", PORT) \
    .load()

# 5. La columna “value” la mantenemos en cadena. 
json_flujo = lines.select("value") 

# 6. Mostramos la columna "value"
resultado = json_flujo.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# 7. Se deja el streaming activada por detención manual.
resultado.awaitTermination()
