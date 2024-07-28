package ofertas_data

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.log4j.{Logger, Level}

import org.apache.spark.rdd.RDD
import scala.util.Properties.isWin

case class OfertasDataArticle(title: String, text: String):
  /**
    * @return Si el texto de este artículo menciona `lang` o no
    * @param lang texto a buscar (por ejemplo, "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)

object OfertasDataRanking extends OfertasDataRankingInterface:
  // Reducir la verbosidad de los registros de Spark
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val langs = List(
  "Python", "R", "SQL", "Java", "Scala", "Julia", "SAS", "MATLAB", "HiveQL", "PigLatin",
  "C++", "JavaScript", "Shell", "Bash", "Perl", "Ruby", "Go", "Kotlin", "TypeScript",
  "C#", "D", "Haskell", "F#", "PHP", "Swift", "Objective-C", "Rust"
)
  /**
   * 
   * CONFIGURAR SPARK:
   * 
   * Para simplificar la logística, se ejecuta Spark en modo "local". 
   * La aplicación Spark completa se ejecutará en un solo nodo, localmente.
   * 
   * SparkContext es el "asa" de su clúster. Una vez que tenga un SparkContext, podrá utilizarlo para crear y poblar RDDs con datos.
   * 
   * Para crear un SparkContext, se necesita crear primero una instancia SparkConf. 
   * Un SparkConf representa la configuración de la aplicación Spark. 
   * Aquí es donde se especifica ejecutar la aplicación en modo "local". 
   * También se nombra la aplicación Spark en este punto. 
   */
  val conf: SparkConf = new SparkConf().setAppName("langRank").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)

  /**
   * LECTURA DE DATOS DE ofertas_data:
   * 
   * Existen varias formas de leer datos en Spark. La forma más sencilla de leer datos es convertir una colección existente en memoria en un RDD utilizando el método parallelize del contexto Spark.
   * Ya hemos implementado un método parse en el objeto OfertasData que analiza una línea del conjunto de datos y la convierte en un OfertasDataArticle.
   * En el siguiente punto, se crea un RDD (implementando val wikiRdd) que contiene los objetos OfertasDataArticle de OfertasData.
   */
  // Pista: use una combinación de `sc.parallelize`, `OfertasData.lines` y `OfertasData.parse`
  val wikiRdd: RDD[OfertasDataArticle] = 
    sc.parallelize(
      for (
        line <- OfertasData.lines;
        article = OfertasData.parse(line)
      ) yield article
    )

  /**
   * CALCULAR UNA CLASIFICACIÓN DE LOS LENGUAJES DE PROGRAMACIÓN:
   * Utilizaremos una métrica sencilla para determinar la popularidad de un lenguaje de programación: 
   * el número de ofertas de ofertas_data que mencionan el lenguaje al menos una vez.
   */
  
  /** 
   * Intento de clasificación de lenguajes nº 1: rankLangs
   *
   * @return El número de ofertas en los que ocurre el lenguaje `lang`.
   * Pista1: considere usar el método `aggregate` en RDD[T].
   * Pista2: considere usar el método `mentionsLanguage` en `OfertasDataArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[OfertasDataArticle]): Int = 
    rdd.filter(_.mentionsLanguage(lang))
      .count().toInt

  /* 
   * Método rankLangs que calcula una lista de pares en la que el segundo componente del par es el número de ofertas que mencionan el lenguaje 
   * (el primer componente del par es el nombre del lenguaje).
   *
   * (1) Use `occurrencesOfLang` para calcular la clasificación de los lenguajes
   *     (`val langs`) determinando el número de ofertas de ofertas_data que
   *     mencionan cada lenguaje al menos una vez. No olvide ordenar los
   *     lenguajes por su ocurrencia, ¡en orden decreciente!
   *
   *   Nota: esta operación es de larga duración. Puede ejecutarse potencialmente durante
   *   varios segundos.
   * 
   * Un ejemplo de lo que podría devolver rankLangs podría ser, por ejemplo, lo siguiente:
   *   List(("Scala", 999999), ("JavaScript", 1278), ("LOLCODE", 982), ("Java", 42))
   * 
   *   La lista debe ordenarse de forma descendente. 
   */
  def rankLangs(langs: List[String], rdd: RDD[OfertasDataArticle]): List[(String, Int)] = 
    langs.map(lang =>
      (lang, occurrencesOfLang(lang, rdd))
    ).sortBy(_._2).reverse

  /* 
   * INTENTO DE CLASIFICACIÓN DE IDIOMAS nº 2: rankLangsUsingIndex
   * 
   * Un índice invertido es una estructura de datos de índice que almacena un mapeo desde un contenido, como palabras o números, a un conjunto de documentos. 
   * En concreto, el propósito de un índice invertido es permitir búsquedas rápidas de texto completo.
   * En nuestro caso de uso, un índice invertido sería útil para establecer correspondencias entre los nombres de los lenguajes de programación y la colección de ofertas de ofertas_data que mencionan el nombre al menos una vez.
   * 
   * @return un índice invertido del conjunto de ofertas, mapeando cada idioma
   * a las páginas de ofertas_data en las que aparece.
   */
  def makeIndex(langs: List[String], rdd: RDD[OfertasDataArticle]): RDD[(String, Iterable[OfertasDataArticle])] = {
    val rawMap = for(
      article <- rdd;
      lang <- langs if article.mentionsLanguage(lang)
    ) yield (lang, article)
    rawMap.groupByKey()
  }

  /* 
   * Cálculo de la clasificación, rankLangsUsingIndex:
   *
   * (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Nota: esta operación es de larga duración. Puede ejecutarse potencialmente durante
   *   varios segundos.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[OfertasDataArticle])]): List[(String, Int)] = 
    index
      .mapValues(s => s.size)
      .collect()
      .sortBy(_._2)
      .reverse
      .toList

  /* 
   * (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Nota: esta operación es de larga duración. Puede ejecutarse potencialmente durante
   *   varios segundos.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[OfertasDataArticle]): List[(String, Int)] =  {
    (for(
      article <- rdd;
      lang <- langs if article.mentionsLanguage(lang)
    ) yield (lang, 1))
      .reduceByKey(_+_)
      .collect()
      .sortBy(_._2)
      .reverse
      .toList
  }

  def main(args: Array[String]): Unit = {
    /* Lenguajes clasificados según (1) */
    val langsRanked: List[(String, Int)] = timed("Parte 1: clasificacion naive", rankLangs(langs, wikiRdd))
    println("\nClasificacion naive:")
    println(langsRanked.map { case (lang, count) => s"($lang,$count)" }.mkString(""))

    /* Un índice invertido que mapea lenguajes a páginas de ofertas_data en las que aparecen */
    def index: RDD[(String, Iterable[OfertasDataArticle])] = makeIndex(langs, wikiRdd)

    /* Lenguajes clasificados según (2), utilizando el índice invertido */
    val langsRanked2: List[(String, Int)] = timed("Parte 2: clasificacion utilizando indice invertido", rankLangsUsingIndex(index))
    println("\nClasificacion utilizando índice invertido:")
    println(langsRanked2.map { case (lang, count) => s"($lang,$count)" }.mkString(""))

    /* Lenguajes clasificados según (3) */
    val langsRanked3: List[(String, Int)] = timed("Parte 3: clasificacion utilizando reduceByKey", rankLangsReduceByKey(langs, wikiRdd))
    println("\nClasificacion utilizando reduceByKey:")
    println(langsRanked3.map { case (lang, count) => s"($lang,$count)" }.mkString(""))

    /* Imprimir la velocidad de cada clasificación */
    println("\n"+timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T =
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Proceso $label tarda ${stop - start} ms.\n")
    result
