package de.hpi.mmds.wiki.lda

import scala.collection.mutable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

class LatentDirichletAllocation(input: String, output: String, num_topics: Int) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    val conf = new SparkConf(true)
    conf.setIfMissing("spark.master", "local[*]") // Run locally by default.
    conf.setAppName(s"lda ($input, $output, num_topics=$num_topics)")
    new SparkContext(conf)
  }
  
  def run() = {
    // Debug
    sc.setLogLevel("ERROR")
    // Load documents
    val (documents, vocab) = load(input)
    
    // Set LDA parameters
    val lda:LDA = new LDA()
        .setK(num_topics)
        .setMaxIterations(10)
        .setOptimizer("online")
    
    val ldaModel = lda.run(documents).asInstanceOf[LocalLDAModel]
    
    ldaModel.save(sc, output)
    
    sc.setLogLevel("INFO")
  }
    
  def load (path: String)
  : (RDD[(Long, Vector)], Array[String]) = {
    val input = sc.textFile(path + ".csv")
    val output = input.map({ s =>
        val elements = s.split(';')
        (elements(0).toLong, Vectors.parse(elements(elements.size - 1)))
    })
    val vocab_rdd = sc.textFile(path + "-vocab.txt")
    val vocab = vocab_rdd.first().split("\\W+")
    (output, vocab)
  }
}

object LatentDirichletAllocation {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: scala <main class> <input file location> <output file location> <number of topics>")
      sys.exit(1)
    }

    val startTime = java.lang.System.currentTimeMillis
    new LatentDirichletAllocation(args(0), args(1), args(2).toInt).run()
    val endTime = java.lang.System.currentTimeMillis

    println(f"Finished in ${endTime - startTime}%,d ms.")  }

}