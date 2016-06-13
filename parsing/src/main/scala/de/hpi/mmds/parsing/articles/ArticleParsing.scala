package de.hpi.mmds.sparking.articles

// import spark stuff
import org.apache.spark.{SparkConf, SparkContext}

// import xml reading stuff
import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}

// import xml transforming stuff
import scala.xml.XML

class ArticleParser(input: String, output: String) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    val conf = new SparkConf(true)
    conf.setIfMissing("spark.master", "local[2]") // Run locally by default.
    conf.setAppName(s"lda ($input) ($output)")
    new SparkContext(conf)
  }
  
  def run() = {
    // DEBUG
    sc.setLogLevel("ERROR")
    
    // configure hadoop
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")
    
    // read file
    val rdd_input = sc.newAPIHadoopFile(input, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
    val rdd_text = rdd_input.map {
      case (writable, text) => (writable.toString, text.toString)
    }

    val rdd_single_line = rdd_text
        .map{ s =>
            val xml = XML.loadString(s._2)
            val id = (xml \ "id").text.toInt
            val title = (xml \ "title").text
            val text = (xml \ "revision" \ "text").text.replaceAll("\\W", " ")
            val tknzed = text.split("\\W").filter(_.size > 3).toList
          (id, title, tknzed )
        }
    
    rdd_single_line.saveAsTextFile(output)
  }
}

object ArticleParser {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: scala <main class> <input file location> <output file location>")
      sys.exit(1)
    }
    new ArticleParser(args(0), args(1)).run()
  }

}