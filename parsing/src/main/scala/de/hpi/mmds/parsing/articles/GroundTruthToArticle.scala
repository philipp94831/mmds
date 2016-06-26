package de.hpi.mmds.parsing.articles

import scala.collection.mutable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

class GroundTruthToArticles(ground_truth: String, articles: String, output: String) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    val conf = new SparkConf(true)
    conf.setIfMissing("spark.master", "local[2]") // Run locally by default.
    conf.setAppName(s"lda ($ground_truth) ($articles) ($output)")
    new SparkContext(conf)
  }
  
  def run() = {
    // DEBUG
    sc.setLogLevel("ERROR")
    
    // load stuff
    val ground_truth_rdd = read_ground_truth(ground_truth)
    val (articles_rdd, vocabArray) = load(articles)
    
    // join
    val join = articles_rdd.join(ground_truth_rdd)
    join.map({
        case Row(id: Int, text: SparseVector)
        => (id.toString + ';' + text)
    })
    
    // Debug output
    println("# of articles: " + join.count)
    
    join.saveAsTextFile(output)
  }
  
  private def read_ground_truth(path: String)
      : RDD[(Long, String)] = {
    val input = sc.textFile(path).map({ s =>
        val text = s.split(",")
        (text(0).toLong, text(1))
    })
    val output = input.reduceByKey((v1, v2) => v1)
    (output)
  }
    
  def load (path: String)
  : (RDD[(Long, Vector)], Array[String]) = {
    val input = sc.textFile(path)
    val output = input.map({ s =>
        val elements = s.split(';')
        (elements(0).toLong, Vectors.parse(elements(elements.size - 1)))
    })
    val vocab_rdd = sc.textFile(path + "-vocab")
    val vocab = vocab_rdd.first().split("\\W+")
    (output, vocab)
  }
}

object GroundTruthToArticles {
  def main(args: Array[String]): Unit = {    
    if (args.isEmpty) {
      println("Usage: scala <main class> <ground_truth file location> <articles file location> <output file directory, nonexistent>")
      sys.exit(1)
    }
    new GroundTruthToArticles(args(0), args(1), args(2)).run()
  }

}