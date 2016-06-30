package de.hpi.mmds.wiki.lda

import org.apache.spark.{SparkConf, SparkContext}

class test(input: String, output: String, num_topics: Int) {
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
    
    // load model
    val recommender = LDA_Recommender.load(sc, output)
    recommender.recommend(991, sc.parallelize(Array(new Integer(3))).toJavaRDD())
  }
}

object test {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: scala <main class> <input file location> <output file location> <number of topics>")
      sys.exit(1)
    }

    val startTime = java.lang.System.currentTimeMillis
    new test(args(0), args(1), args(2).toInt).run()
    val endTime = java.lang.System.currentTimeMillis

    println(f"Finished in ${endTime - startTime}%,d ms.")  }

}