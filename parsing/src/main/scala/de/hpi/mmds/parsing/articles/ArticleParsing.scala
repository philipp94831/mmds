package de.hpi.mmds.sparking.articles

import org.apache.hadoop.streaming.StreamXmlRecordReader
import org.apache.hadoop.streaming.StreamInputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.XML

class ArticleParser(input: String) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    val conf = new SparkConf(true)
    conf.setIfMissing("spark.master", "local[2]") // Run locally by default.
    conf.setAppName(s"lda ($input)")
    new SparkContext(conf)
  }
  
  def run() = {
    // configure reading
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class",
                "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<page")
    jobConf.set("stream.recordreader.end", "</page>")
    FileInputFormat.addInputPaths(jobConf,
                                  s"C:/Users/Marianne/Documents/Uni/HPI/Semester_1/MMDS-Mining_Massive_Datasets/data/enwiki-20160501-pages-articles1-20articles.xml-p000000010p000030302")
    
    val documents = sc.hadoopRDD(jobConf,	classOf[org.apache.hadoop.streaming.StreamInputFormat],
                                          classOf[org.apache.hadoop.io.Text],
                                          classOf[org.apache.hadoop.io.Text])
    val texts = documents.map(_._1.toString)
                          .map{ s => 
                            val xml = XML.loadString(s)
                            val id = (xml \ "id").text.toDouble
                            val title = (xml \ "title").text
                            val text = (xml \ "revision" \ "text").text.replaceAll("\\W", " ")
                            val tknzed = text.split("\\W").filter(_.size > 3).toList
                            (id, title, tknzed )
                          }
    texts.saveAsTextFile(s"C:/Users/Marianne/Documents/Uni/HPI/Semester_1/MMDS-Mining_Massive_Datasets/data/enwiki-20160501-pages-articles1-20articles.txt")
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