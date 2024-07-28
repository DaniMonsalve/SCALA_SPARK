# SCALA_SPARK
Este proyecto es una aplicación en Scala que utiliza Apache Spark para clasificar ofertas de empleo según los lenguajes de programación mencionados en los textos. El objetivo es demostrar diferentes técnicas de clasificación y procesamiento de datos en grandes volúmenes. En este sentido, el programa utiliza Spark para explorar los lenguajes de programación más mencionados en las ofertas de trabajo publicadas en LinkedIn. La información utilizada se extrae de un fichero de texto completo, con información previamente descargada mediante web scraping (este proceso se encuentra disponible en el repositorio "ETL-WebScraping-Selenium-NPL").

	# REQUISITOS:
Scala 3.3.0

SBT (Scala Build Tool)

Apache Spark

Java 8 o superior


	# ESTRUCTURA DEL PROYECTO:
src/main/scala/ofertas_data/OfertasDataRanking.scala: Contiene la lógica principal del programa, incluyendo la configuración de Spark y los métodos para clasificar las ofertas.

build.sbt: Archivo de configuración de SBT que define las dependencias y configuraciones del proyecto.

	#DETALLES Y OBJETIVOS DEL PROGRAMA:

El programa realiza las siguientes tareas:

-Clasificación naive: Clasifica los lenguajes de programación contando su aparición directa en las ofertas.

-Índice invertido: Construye un índice que mapea lenguajes a ofertas en las que aparecen, y luego clasifica los lenguajes usando este índice.

-ReduceByKey: Utiliza el método reduceByKey de Spark para contar las apariciones de los lenguajes de programación de manera eficiente.


	# RESULTADO DE LA EJECUCIÓN:
 <img width="944" alt="image" src="https://github.com/user-attachments/assets/4ced0cdc-a2db-4b26-9690-c2969dacb083">

	# CONFIGURACIÓN DE SPARK:
 
Para simplificar la logística, se ejecuta Spark en modo "local", ejecutando la aplicación en un solo nodo. Para empezar, se crea un SparkContext, creando para ello una instancia SparkConf.

-SparkConf representa la configuración de la aplicación Spark:
	-Se especifica ejecutar la aplicación en modo "local".
	-También se nombra la aplicación Spark en este punto.

	# LECTURA DE DATOS:
 
Para leer los datos en Spark, se convierte una colección existente en memoria en un RDD utilizando el método parallelize.
También se implementa un método parse en el objeto OfertasData que analiza una línea del conjunto de datos y la convierte en un OfertasDataArticle.
Se crea un RDD(implementando val wikiRdd) que contiene los objetos OfertasDataArticle de OfertasData.

	# CLASIFICACIÓN DE LOS LENGUAJES REALIZADA:

Utilizaremos una métrica sencilla para determinar la popularidad de un lenguaje de programación: el número de ofertas que mencionan el lenguaje al menos una vez.

-Intento de clasificación de lenguajes nº 1: rankLangs
Cálculo deoccurrencesOfLang

Un método de ayuda occurrencesOfLang que calcula el número de ofertas en un RDD de tipo RDD[OfertasDataArticles] que mencionan el lenguaje dado al menos una vez. Para simplificar, se comprueba que al menos una palabra (delimitada por espacios) del texto contenga un texto dado.

Utilizando occurrencesOfLang, se implementa un método rankLangs que extrae una lista de pares en la que el segundo componente del par es el número de ofertas que mencionan el nombre dado (el primer componente del par es el nombre dado).

-Intento de clasificación de idiomas nº 2: rankLangsUsingIndex
Calcular un índice invertido

Un índice invertido es una estructura de datos de índice que almacena un mapeo desde un contenido, como palabras o números, a un conjunto de documentos. En concreto, el propósito de un índice invertido es permitir búsquedas rápidas de texto completo. En este caso, un índice invertido es útil para establecer correspondencias entre los nombres de los lenguajes de programación y la colección de ofertas que mencionan el nombre al menos una vez.

la lista se ordena de forma descendente. El par con el segundo componente más alto (el recuento) es el primer elemento de la lista.

-Lenguajes de rango intento #3: rankLangsReduceByKey
En el caso de que el índice invertido de arriba sólo se utilice para calcular la clasificación y para ninguna otra tarea (búsqueda de texto completo, por ejemplo), es más eficiente utilizar el método reduceByKey para calcular la clasificación directamente, sin calcular primero un índice invertido. 

Utilizando reduceByKey, al igual que en las partes 1 y 2, rankLangsReduceByKey calcula una lista de pares en la que el segundo componente del par es el número de ofertas que mencionan el texto dado (el primer componente del par es el nombre dado).

De nuevo, la lista se clasifica en orden descendente. 

