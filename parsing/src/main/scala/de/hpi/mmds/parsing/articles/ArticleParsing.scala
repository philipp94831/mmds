package de.hpi.mmds.sparking.articles

import com.databricks.spark.xml.XmlInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.{LongWritable, Text}

class ArticleParser(input: String) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    val conf = new SparkConf(true)
    conf.setIfMissing("spark.master", "local[2]") // Run locally by default.
    conf.setAppName(s"lda ($input)")
    new SparkContext(conf)
  }
  
  def run() = {
    // configure hadoop
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")
    
    val path = "C:/Users/Marianne/Documents/Uni/HPI/Semester_1/MMDS-Mining_Massive_Datasets/data/enwiki-20160501-pages-articles1-20articles.xml-p000000010p000030302"
    
    // read file
    val records = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])

    
    
    
    
    
    
    
    // DEBUG
    sc.setLogLevel("ERROR")
    records.foreach(println)
    
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("\n")
    println("Number of records: ")
    println(records.count())
    println("\n")
  }
}

object ArticleParser {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: scala <main class> <input file location>")
      sys.exit(1)
    }

    val startTime = java.lang.System.currentTimeMillis
    new ArticleParser(args(0)).run()
    val endTime = java.lang.System.currentTimeMillis

    println(f"Finished in ${endTime - startTime}%,d ms.")  }

}