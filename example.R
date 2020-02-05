

# # Ejemplo codigo en R (sparkly)

# ## Basico

# En un script de R en CDSW, incluir los comentariosIn
# como se haría en cualquier codigo R

print("Hello world!")

1 + 1

# Cuando se corre una parte o todo del script 
# los comentarios igual son mostrados en la sesion
# de la consola 

# Para correr comandos shells en sistema operativo
# usar `system()`. :

system("ls -l")


# ## Markdown  - Comentarios


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


# ## Copiar archivos a HDFS

# El proyecto incluye un dataset que describe
# el perfomance de los vuelos de salida de New York
# de los aeropuertos (EWR, JFK, and LGA) anio 2013
# fuente U.S. Department of Transportation. 
# formato archivos csv, bajo el nombre `flights.csv`.

# borrar la carpeta flights 

system("hdfs dfs -rm -r flights")

# crear el sbudirectorio  `flights` :

system("hdfs dfs -mkdir flights")

# copiar el archivo local al hdfs:

system("hdfs dfs -put flights.csv flights/")

# verificar

system("hdfs dfs -ls  flights/")

# ## Usando Apache Spark 2 con sparklyr

# Acceso al cluster usando
# [sparklyr](https://spark.rstudio.com) as the R

# Instalar sparklyr desde CRAN (si no ha sido 
# instalado anteriormente:

if(!"sparklyr" %in% rownames(installed.packages())) {
  install.packages("sparklyr")
}

# Antes de conectarte a Spark: Si se usa un usuario que esta asegurado
# sobre el cluster con autenticacion Kerberos, debes ir a 
# la seccion de Hadoop Authentication section del usuario de CDSW 
# configurar y entrar el usuario y password de kerberos Kerberos


# ### Conectando a Spark

# leer el paquete sparklyr :

library(sparklyr)

# Then call the `spark_connect()` funcion para conectarse
# a Spark, en este caso sobre yarn dandole un nombre a 
# la aplicacion Spark:

spark <- spark_connect(
  master = "yarn",
  app_name = "cdsw-training-cvillarroel"
)

# esa variable  `spark` la usaras para ler datos desde Spark


# ### Leer los datos



flights <- spark_read_csv(
  sc = spark,
  name = "flights",
  path = "flights/",
  header = TRUE,
  infer_schema = TRUE
)

# El resultado es un Data Frame llamado  `flights`. 
# Notar que esto no es un dataframe de R es un puntero
# a un Dataframe Spark




# ### Analizar los datos (inspeccionar)

# Inspeccionar el DataFrame Inspecctor Inspect the Spark DataFrame to gain a basic
# understanding of its structure and contents.

# Hacer el código mas legible usnado el operador de 
# pipe  `%>%`.

# imprimir el número de filas:

flights %>% sdf_nrow()

# Imprimir las columnas disponibles:

flights %>% colnames()

# por defecto imprime las 10 primeras filas 
# pero se puede generar un quiebre si hay muchas columnas
# (este es el comportamiento por omision):

flights

# para mostrar todas las columnas, 
# aun si las filas se muestran o intercalan en 
# multiples lineas, setear el parámetro con Inf
#`width = Inf`:

flights %>% print(n = 5, width = Inf)

# ### Transformando los datos usando verbos dplyr 

# sparklyr trabaja en conjunto con el paquete popular
# [dplyr](http://dplyr.tidyverse.org). sparklyr habilita 
# usar dplyr *verbs* para manipular los datos con Spark.

# Las instrucciones mas comunes para manipular datos:
# * `select()` seleccionar filas
# * `filter()` filtrar filas
# * `arrange()` ordenar filas
# * `mutate()` crear nuevas columnas 
# * `summarise()` agregar


# `rename()` and `transmute()` variaciones de los 
# verbos principales.


# `group_by()`, operaciones de agrupacion.

# leer el paquete dplyr:

library(dplyr)


# `select()` retorna columnas especificas:

flights %>% select(carrier)



# forma desorganizada

select(flights, year:day, arr_delay, dep_delay)

# forma con el operador %>%

flights %>% select(year:day, arr_delay, dep_delay)

# `distinct()` trabaja como `select()` pero retorna solo
# valores distintos:

flights %>% distinct(carrier)

# `filter()` retorna las filas que satisfacen
# la expresión booleana:

flights %>% filter(dest == "SFO")

# `arrange()` retorna las filas arregladas(ordinadas)
# por las columnas específicas:

flights %>% arrange(month, day)

# El orden por defecto es ascendente por lo que 
# se usa la función de ayuda `desc()` para ordenar una columna
# por el orden descendente:

flights %>% arrange(desc(month), desc(day))

# `mutate()` agrega nuevas columnas o reemplaza las adds new columns or replaces existing
# columns using the specified expressions:

flights %>% mutate(on_time = arr_delay <= 0)

flights %>% mutate(flight_code = paste0(carrier, flight)) %>% select(flight_code)

# `summarise()` ejecuta agregaciones con las columnas especificas.

# funciones de agregacion `n()`, `n_distinct()`,
# `sum()`, and `mean()`. con algunas funciones se debe,
# especificar `na.rm = TRUE` para no mostrar alertas
# por falta de datos:

flights %>% summarise(n = n())

flights %>%
  summarise(num_carriers = n_distinct(carrier))

# `group_by()` agruga datos para una columna especifica 
# las agregaciones pueden (usando summarise) pueden 
# calcularse por grupos ejemplo:

flights %>%
  group_by(origin) %>%
  summarise(
    num_departures = n(),
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
  )

# se puede encadenar las instrucciones dplyr verbs:

flights %>%
  filter(dest == "BOS") %>%
  group_by(origin) %>%
  summarise(
    num_departures = n(),
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
  ) %>%
  arrange(avg_dep_delay)


# ### usando SQL querys directamente

# En vez de usar dplyr verbs, se puede usar SQL query
# para obtener el mismo resultado:

tbl(spark, sql("
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'BOS'
  GROUP BY origin
  ORDER BY avg_dep_delay"))


# ### Funciones para Spark DataFrames 

# además de los dplyr verbs, hay otras funciones:

# `na.omit()` filtrar filas que contengan un valor nulo:

flights %>% na.omit()

# `sdf_sample()`  retorna una muestra de filas aleatorias:

flights %>%
  sdf_sample(fraction = 0.05, replacement = FALSE)


# ### Visualizando datos desde Spark

# se puede visualizar datos usando la libreria ggplot2.

# se debe de pasar del dataframe de spark a un data frame de R 
# para esto usar `collect()` funcion.

# precaucion: recomendacion filtrar, muestrear o agregar 
# antes de usar `collect()` para retornar un data frame normal
# de R.


delays_sample_df <- flights %>%
  select(dep_delay, arr_delay) %>%
  na.omit() %>%
  sdf_sample(fraction = 0.05, replacement = FALSE) %>%
  collect()

# crear el diagrama de dispersion
# relacion entre  departure delay y arrival delay:

library(ggplot2)

ggplot(delays_sample_df, aes(x=dep_delay, y=arr_delay)) +
  geom_point()




# ### Machine Learning con MLlib

# MLlib es la libreria de machine learning de spark.

# Esta particion aleatoria de los datos ensamblados
# genera una 
# Muestra aleatoria (70% registros) y una para pruebas (30% de
# registros):

flights_to_model <- flights %>%
  select(dep_delay, arr_delay) %>%
  na.omit()



samples <- flights_to_model %>%
  sdf_partition(train = 0.7, test = 0.3)

# especificar en la regresion lineal los datos de entrenamiento 
# de la muestra obtenida :

model <- samples$train %>%
  ml_linear_regression(arr_delay ~ dep_delay)

# Examinar el modelo respecto al  Examine the model intercepcion en el origen
# y los coeficientes

summary(model)

# usar el modelo para generar predicciones sobre el test:

pred <- model %>%
  ml_predict(samples$test)

# Evaluar el modelo usando 
# R cuadrado (R-squared) es la fracción de la varianza
# en la prueba test que es explicada por el modelo:


pred %>%
  summarise(r_squared = cor(arr_delay, prediction)^2)


# ### limpieza

# desconectarse del Spark:

spark_disconnect(spark)
