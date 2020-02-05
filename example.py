

# # Ejemplo de codigo python para ejecutar el CDSW

# ## Basicos

# Puedes correr tanto los comentarios como el código en Markdown
# script sencillo

print("Hello world!")

1 + 1

# paso previo crear los usuarios en hdfs 
# una alternativa es que el usuario admin,  los cree en HUE


# CDSW tiene soporte a comandos Jupyter para usar shell y comandos de sistema operativo

!ls -l

# Para mas detalles
# [la documentacion para comandos de Jupyter](https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_jupyter.html).

# cargar un archivo desde la web
!curl -o airlines.csv https://s3.amazonaws.com/cdsw-training/airlines.csv

#listar el archivo generado desde internet

!ls -l airlines.csv



# ## Markdown

# [Markdown](https://daringfireball.net/projects/markdown/syntax).
# Ejemplo:

# # Heading 1

# ## Heading 2

# ### Heading 3

# Plain text

# *Emphasized text*

# **Bold text**

# `Monospaced text`

# Bulleted list
# * Item
#   * Subitem
# * Item

# Numbered list
# 1. First Item
# 2. Second Item
# 3. Third Item

# [link cloudera](https://www.cloudera.com)



# los comentarios en la line
# [Markdown](https://daringfireball.net/projects/
# Este archivo incluye los vuelos que salen de New York,  los aeropuertes de arribo
# rendimiento de los vuelos que parten desde NY 
# (EWR, JFK, and LGA) para el año 2013. Estos datos
# en el archivo `flights.csv`.

# copiar el archivo con el comando `hdfs dfs` 
# CDSW dentro de la línea de comandos:

# Borrar el directorio `flights` 
# que esta en tu home directoy en caso de :

!hdfs dfs -rm -r flights

# crear el directorio `flights`:

!hdfs dfs -mkdir flights

# poner el archivo de vuelos en el directorio hdfs (corregir la ruta de origen):

!hdfs dfs -put flights.csv flights/

# poner el archivo flights.csv   que ahora esta en el directorio `data/flights`
# `flights` en tu directorio home del HDFS en la carpeta flights.

!hdfs dfs -ls flights/



!pip install folium

import folium

# ## Usar Apache Spark 2 con PySpark

# CDSW provee un gateway virtual hacia el cluster
# que tu puedes usar para correr Apache Spark usando pyspark
# Spark's Python API.

# Antes de conectarte a Spark por el cluster: si estas usando un 
# cluster con autenticacion Kerberos, primero debes ir a la sección 
# de autenticar tu usuario Hadoop en CDSW indicado anteriormente 
# configurar y entrar el usuario y password de Kerberos 


# ### Connecting to Spark

# Spark SQL es el módulo de Spark para trabajar con datos estructurados
# PySpark es la API de Python de Spark. El modulo `pyspark.sql`
#  expone la funcionalidad de Spark SQL a Python.

# Importando el punto inicial `SparkSession`, PySpark's main entry

from pyspark.sql import SparkSession

# cuando llamas al metodo `getOrCreate()` que pertenece a
# `SparkSession.builder` para conectarse a Spark. Este
# ejemplo conecta Spark sobre Yarn y da un nombre a la 
#aplicación spark :

spark = SparkSession.builder \
  .master("local") \
  .appName("cdsw-training") \
  .getOrCreate()

# ahora puede usarse la  `SparkSession` llamada `spark` para leer
# datos dentro de spark.


# ### Leer datos

# Leer el data set que esta con formato CSV
#  el 
# esquema de datos se genera automaticamente a partir de los datos:

flights = spark.read.csv("flights/", header=True, inferSchema=True)

# El resultado es un DataFrame de spark llamado `flights`.


# ### Inspeccionar los datos

# Analizar el Dataframe para ver la estructura y contenido


# imprimir el numero de columnas:

print(flights.count())

# Imprimir el esquema:

flights.printSchema()

# Analizar una o más variables (columnas) :

flights.describe("arr_delay").show()
flights.describe("arr_delay", "dep_delay").show()

# Imprimir las primeras 5 filas :

flights.limit(5).show()

# o mas consisamente:

flights.show(5)

# Por defecto show muestra las 20 primeras filas

flights.show()

# `show()` no es muy versatil cuando hay muchas columnas
#  para eso es preferible mandar una muestra de x filas
# a un dataframe de Pandas que es otra estructura que se
# usa para ser usado en algoritmos de machine learning:

flights_pd = flights.limit(5).toPandas()
flights_pd

# Para mostrar el dataframe de pandas con un scroll 
# grid se puede usar el seteo para mostrarlos como 
# una tabla html con este comando :

import pandas as pd
pd.set_option("display.html.table_schema", True)

flights_pd

# Precaucion: cuando se trabaja con SPark dataframe 
# limitar el numero de filas que retornara la consulta
# antes de mandarlo al Pandas


# ### Transformar Datos

# Spark SQL provee un conjunto de funciones para
# manipular Spark Dataframes,  cada método retorna
# un nuevo DataFrame 

# `select()` retorna la columna específica:

flights.select("carrier").toPandas().head()

# `distinct()` retorna filas distintas por la columna 
# específica:

flights.select("carrier").distinct()

# `filter()` (or su alias `where()`) retorna las filas
# que satisfacen las condiciones.

# se usa una cuncion col para hacer referencia a una columna
# y una columna lit para valores literales :

from pyspark.sql.functions import col, lit

flights.filter(col("dest") == lit("SFO")).show()

# version where

flights.where(col("dest") == lit("SFO")).show()

#otra opcion dentro del where

flights.where("dest = 'SFO'").show()

# Recordar que los dataFrames son inmutables

#para verlo mejor

flights.filter(col("dest") == lit("SFO")).limit(10).toPandas()

# `orderBy()` (or its alias `sort()`) returns rows
# arranged by the specified columns:

flights.select("year","day","flight").orderBy("month", "day").show()

flights.select("year","day","month","flight").orderBy("month", "day").show()

flights.select("year","day","month","flight").orderBy("month", "day", ascending=False).show()

# escoger un campo

flights \
   .filter(col("dest") == lit("SFO")) \
   .orderBy("month", "day", ascending=False) \
   .select("month", "day","origin","dep_delay") \
   .show()


    
    
# `withColumn()` agrega una nueva columna o reemplaza
#  una existente
#  usando la expresión especificada:

flights \
  .withColumn("on_time", col("arr_delay") <= 0) \
  .show()

flights.show(5) 
  
  
# para concatenar cadenas, importar la funcion
# `concat()`:

from pyspark.sql.functions import concat

flights=flights \
  .select("flight","carrier") \
  .withColumn("flight_code", concat("carrier", "flight")) \
  .select("flight_code")
  
flights.toPandas().head(10)
  
# `agg()` ejecutar agregaciones usando expresiones
# especificas

# la sentencia agg() te permite crear un Dataframe agregado

# importar y usar funciones de agregacion como `count()`,
# `countDistinct()`, `sum()`, and `mean()`:

from pyspark.sql.functions import count, countDistinct

flightsagg=flights.agg(count("*"))
type(flightsagg)

flights.agg(countDistinct("carrier")).show()

# usar el metodo de una columna llamado `alias()` 
# para asignar un nombre a la columna resultado:

flights \
  .agg(countDistinct("carrier").alias("num_carriers")) \
  .show()

# `groupBy()` agrupa datos por columnas especificas 
# las agregaciones pueden ser calculadas por grupos:

from pyspark.sql.functions import mean

flights \
  .groupBy("origin") \
  .agg( \
       count("*").alias("num_departures"), \
       mean("dep_delay").alias("avg_dep_delay") \
  ) \
  .show()
  
# para verlo mejor

flights \
  .groupBy("origin") \
  .agg( \
       count("*").alias("num_departures"), \
       mean("dep_delay").alias("avg_dep_delay") \
  ) \
  .toPandas()

# Pueden encadenar multiples métodos de los Dataframes:

flights \
  .filter(col("dest") == lit("BOS")) \
  .groupBy("origin") \
  .agg( \
       count("*").alias("num_departures"), \
       mean("dep_delay").alias("avg_dep_delay") \
  ) \
  .orderBy("avg_dep_delay") \
  .show()

# mandar el resultado a un objeto Dataframe  
nyc_bos_dep_delay_pd= flights \
  .filter(col("dest") == lit("BOS")) \
  .groupBy("origin") \
  .agg( \
       count("*").alias("num_departures"), \
       mean("dep_delay").alias("avg_dep_delay") \
  ) \
  .orderBy("avg_dep_delay") 


# verlo en Pandas  
nyc_bos_dep_delay_pd.toPandas()
  

# ### Usando querys SQl

# En vez de usar métodos Spark DataFrame 
# usar un query que consiga el mismo resultado 


# Primero se debe de crear una tabla temporal
# a partir del dataframe 
# que sea vista por el contexto de Spark


flights.createOrReplaceTempView("flights")

# para usarlo dentro de un query:

spark.sql("""
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'BOS'
  GROUP BY origin
  ORDER BY avg_dep_delay""").show()


# ### Visualizando datos desde spark

# se puede usar librerías como Matplotlib

# En el caso de usar matplotlib 
# es necesario usar esta sentencia de Jupyter comando magic
# para  asegurar que las imagenes se muestren bien:

%matplotlib inline

# para  visualizar usando matplotlib se debe
# pasar el dataframe de spark a Pandas primero.

# Se debe de filtrar o limitar el número de registros
# representativos para usar Pandas y Matplotlib
# tanto para mejorar la visualización como para evitar
# colapsar el dataframe de Pandas.

# Por ejemplo sacar el select de departure delay 
# y la demora de arribo, del dataset 'flights'
# se saca una muestra aleatoria del 5% de valores no nulos
# y retornar el resultado como un Dataframe pandas

delays_sample_pd = flights \
  .select("dep_delay", "arr_delay") \
  .dropna() \
  .sample(withReplacement=False, fraction=0.05) \
  .toPandas()

# Crear un diagrama de dispersion que muestre una
# posible relacion entre la demora de despego y la 
# demora de arribo :

delays_sample_pd.plot.scatter(x="dep_delay", y="arr_delay")


#  mejorar el gráfico filtrando más datos de 
# demoras que van mas allá de las 5 horas

delays_sample_pd = flights \
  .filter ((col("dep_delay")<300) & (col("arr_delay")<300)) \
  .select("dep_delay", "arr_delay") \
  .dropna() \
  .sample(withReplacement=False, fraction=0.05) \
  .toPandas()

# grafico los datos filtrados

delays_sample_pd.plot.scatter(x="dep_delay", y="arr_delay")


# Se muestra una relacion positiva entre las dos 
# variables 

# ejemplo hacer otro ejercicio de Barras usando 
# una estructura de agrupamiento 



def explore(df, feature, labelx, labely, plot=True):
  from pyspark.sql.functions import count, mean
  aggregated = df.groupby(feature).agg(count(label), mean(label)).orderBy(feature)
  aggregated.show()
  if plot == True:
    pdf = aggregated.toPandas()
    pdf.plot.bar(x=pdf.columns[0], y=pdf.columns[2], capsize=5)
  
 pdf = nyc_bos_dep_delay_pd.toPandas()
 
 pdf.plot.bar("origin", "avg_dep_delay", capsize=5)
 
  


# ### Machine Learning con MLlib breve ejercicio con regresion

# MLlib es una libreria de  Spark

# Como ejemplo vamos a examinar  la relacion 
# entre la demora en la salida y la demora en arribo 
# usando un modelo de regresion lineal 


# Creamos primero un Spark dataframe con los datos relevantes
# removiendo valores missing :

flights_to_model = flights \
  .select("dep_delay", "arr_delay") \
  .dropna()

# MLlib requiere que todas las columnas de entrada (feature) 
# sea combinada en una sola columna tipo vector, para hacer esto
# importar el transformador de features `VectorAssembler` :

from pyspark.ml.feature import VectorAssembler

# En este ejemplo, aqui solo hay un predictor o entrada o caracteristica 
# variable: `dep_delay`.

assembler = VectorAssembler(inputCols=["dep_delay"], outputCol="features")

# usar el objeto `VectorAssembler` para ensamblar los datos:

flights_assembled = assembler.transform(flights_to_model)
flights_assembled.show(5)
type(flights_assembled)

# Esta particion aleatoria de los datos ensamblados
# genera una 
# Muestra aleatoria (70% registros) y una para pruebas (30% de
# registros):

(train, test) = flights_assembled.randomSplit([0.7, 0.3])

# Importar y usar la libreria `LinearRegression` especificar la
# regresion lineal, entrenar usando la muestra para entrenamiento

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="arr_delay")

lr_model = lr.fit(train)

# Examinar el modelo respecto al  Examine the model intercepcion en el origen
# y los coeficientes

lr_model.intercept

lr_model.coefficients

# Evaluar el modelo lineal con una prueba 
# usando los valores de test:

lr_summary = lr_model.evaluate(test)

# R cuadrado (R-squared) es la fracción de la varianza
# en la prueba test que es explicada por el modelo:

lr_summary.r2

# ### Limpieza

# Desconectarnos del spark:

spark.stop()
