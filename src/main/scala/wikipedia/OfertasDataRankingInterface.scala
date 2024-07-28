package ofertas_data

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD

/**
 * The interface used by the grading infrastructure. Do not change signatures
 * or your submission will fail with a NoSuchMethodError.
 */
trait OfertasDataRankingInterface:
  def makeIndex(langs: List[String], rdd: RDD[OfertasDataArticle]): RDD[(String, Iterable[OfertasDataArticle])]
  def occurrencesOfLang(lang: String, rdd: RDD[OfertasDataArticle]): Int
  def rankLangs(langs: List[String], rdd: RDD[OfertasDataArticle]): List[(String, Int)]
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[OfertasDataArticle]): List[(String, Int)]
  def rankLangsUsingIndex(index: RDD[(String, Iterable[OfertasDataArticle])]): List[(String, Int)]
  def langs: List[String]
  def sc: SparkContext
  def wikiRdd: RDD[OfertasDataArticle]
