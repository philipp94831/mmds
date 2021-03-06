package de.hpi.mmds.wiki.lda

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

@Deprecated
class LatentDirichletAllocation(input: String, output: String, num_topics: Int) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    val conf = new SparkConf(true)
    conf.setIfMissing("spark.master", "local[2]") // Run locally by default.
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
        .setOptimizer("em")
    
    val ldaModel = lda.run(documents).asInstanceOf[DistributedLDAModel]
    
    // Debug output
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 3)
    val topics = topicIndices.map({case (terms, termWeights) =>
        terms.zip(termWeights).map ({ case (term, weight) =>
          (vocab(term.toInt), weight)
        })
    })
    topics.zipWithIndex.foreach { case (topic, i) =>
        println(s"TOPIC $i")
        topic.foreach { case (term, weight) =>
            println(s"$term\t$weight")
        }
        println()
    }
    val topicAssignments = ldaModel.topDocumentsPerTopic(maxDocumentsPerTopic = 3)
    topicAssignments.foreach { case (docs, docWeights) =>
      println("ASSIGNMENTS:")
      docs.zip(docWeights).foreach { case (doc, dweight) =>
        println(s"${doc.toLong}\t$dweight")
      }
      println()
    }
    
    ldaModel.save(sc, output)
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

@Deprecated
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