package de.hpi.isg.mmds.spark

import scala.collection.mutable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

class LDATest(input: String, topics: Int) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    val conf = new SparkConf(true)
    conf.setIfMissing("spark.master", "local[2]") // Run locally by default.
    conf.setAppName(s"lda ($input, topics=$topics)")
    new SparkContext(conf)
  }
  
  def run() = {
    // Load documents from text files, 1 document per file
    val corpus: RDD[String] = sc.wholeTextFiles(input + "/*.txt").map(_._2)
    
    // Split each document into a sequence of terms (words)
    val tokenized: RDD[Seq[String]] =
      corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
    
    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
      tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    //   vocabArray: Chosen vocab (removing common terms)
    val numStopwords = 20
    val vocabArray: Array[String] =
      termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
    
    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }
    
    // Set LDA parameters
    val lda = new LDA().setK(topics).setMaxIterations(10).setOptimizer("em")
    
    val ldaModel = lda.run(documents)
    //val avgLogLikelihood = ldaModel.logLikelihood / documents.count()
    var dldaModel:DistributedLDAModel = ldaModel match {
      case x:DistributedLDAModel => x
      case _ => throw new ClassCastException
    }
    
    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 3)
    topicIndices.foreach { case (terms, termWeights) =>
      println("TERMS:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${vocabArray(term.toInt)}\t$weight")
      }
      println()
    }
    // Print assignments - find out how to map ID to file
    //val topicAssignments = dldaModel.topDocumentsPerTopic(maxDocumentsPerTopic = 3)
    //topicAssignments.foreach { case (docs, docWeights) =>
    //  println("ASSIGNMENTS:")
    //  docs.zip(docWeights).foreach { case (doc, dweight) =>
    //    println(s"${doc.toLong}\t$dweight")
    //  }
    //  println()
    //}
  }
}

object LDATest {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: scala <main class> <input file location> <number of topics>")
      sys.exit(1)
    }

    val startTime = java.lang.System.currentTimeMillis
    new LDATest(args(0), args(1).toInt).run()
    val endTime = java.lang.System.currentTimeMillis

    println(f"Finished in ${endTime - startTime}%,d ms.")  }

}