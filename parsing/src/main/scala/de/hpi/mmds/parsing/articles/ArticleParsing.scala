package de.hpi.mmds.parsing.articles

// import spark stuff
import org.apache.spark.{SparkConf, SparkContext}

// import xml stuff
import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import scala.xml.XML

// import text transformation stuff
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

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
    
    val rdd_input = read_files(sc, input)
    val (rdd_stopwords, vocabArray) = remove_stopwords(rdd_input, 10000)
    
    var vocab_string = vocabArray.mkString(",")
    var vocab_array = Array(vocab_string)
    val rdd_vocab = sc.parallelize(vocab_array)
    
    rdd_stopwords.saveAsTextFile(output)
    rdd_vocab.saveAsTextFile(output + "-vocab")
  }
  
  private def read_files(
      sc: SparkContext,
      path: String)
      : (RDD[(Int, String, String)]) = {  
    // configure hadoop
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
    sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")
    
    // read file
    val rdd_input = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])

    // transform rdd to usable format
    val rdd_preprocessing = rdd_input
      .map({ s =>
          val xml = XML.loadString(s._2.toString)
          val id = (xml \ "id").text.toInt
          val ns = (xml \ "ns").text.toInt
          val title = (xml \ "title").text
          val text = (xml \ "revision" \ "text").text
        (id, ns, title, text )
      })
      .filter(s => s._2 == 0)
      .filter(s => !s._4.startsWith("#REDIRECT"))
      .map({ s =>
          val replaced = s._4.replaceAll("\\W", " ")
          val tknzed = replaced.split("\\W").filter(_.size > 3)
          val trimmed = tknzed.mkString(" ")
        (s._1, s._3, trimmed)
      })
    (rdd_preprocessing)
  }
  
  private def remove_stopwords(
      rdd_before : RDD[(Int, String, String)],
      vocabSize : Int)
      : (RDD[(String)], Array[String]) = {
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val df = rdd_before.toDF("id", "title", "text")
    val tokenizer = new RegexTokenizer()
        .setInputCol("text")
        .setOutputCol("rawTokens")
    val stopWordsRemover = new StopWordsRemover()
        .setInputCol("rawTokens")
        .setOutputCol("tokens")
    val countVectorizer = new CountVectorizer()
        .setVocabSize(10000)
        .setInputCol("tokens")
        .setOutputCol("features")
    val pipeline = new Pipeline()
        .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))
    val model = pipeline.fit(df)
    
    val rdd_after = model
        .transform(df)
        .select("id", "title", "features")
        .rdd
        .map({
            case Row(id: Int, title: String, text: SparseVector)
            => (id + ';' + title + ';' + text)
        })
    
    (rdd_after, model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary)
  }
}

object ArticleParser {
  def main(args: Array[String]): Unit = {    
    if (args.isEmpty) {
      println("Usage: scala <main class> <input file location> <output file directory, nonexistent>")
      sys.exit(1)
    }
    new ArticleParser(args(0), args(1)).run()
  }

}