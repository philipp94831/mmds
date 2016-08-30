package de.hpi.mmds.parsing.articles

// import spark stuff
import java.io.File
import java.nio.charset.StandardCharsets

import com.beust.jcommander.{JCommander, Parameter}
import de.hpi.mmds.wiki.Spark
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

// import xml stuff
import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}

import scala.xml.XML

// import text transformation stuff
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class ArticleParser(input: String, output: String) {
  val sc = {
    // If we use spark-submit, the SparkContext will be configured for us.
    new Spark(s"lda ($input) ($output)").setMaster("local[4]").setWorkerMemory("2g").context()
  }

  def run() = {
    // DEBUG
    sc.setLogLevel("ERROR")

    val parsed = parse(sc, input)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val (vectorized, vocab) = vectorize(parsed, 10000)

    vectorized
      .persist(StorageLevel.MEMORY_AND_DISK)

    Spark.saveToFile(vectorized, output)

    FileUtils.writeStringToFile(new File(output + "-vocab.txt"), vocab.mkString(","), StandardCharsets.UTF_8)
  }

  private def parse(sc: SparkContext, path: String): (RDD[(Integer, String)]) = {
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
        new Article(id, text, title, ns)
      })
      .filter(_.getNamespace == 0)
      .filter(!_.isRedirect)
      .filter(!_.isDisambugation)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .toJavaRDD().map(new AdvancedWikiTextParser).rdd.filter(_ != null)

    rdd_preprocessing
  }

  private def vectorize(parsed: RDD[(Integer, String)], vocabSize: Int): (RDD[(String)], Array[String]) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val textCol = "text"
    val rawTokensCol = "rawTokens"
    val tokensCol = "tokens"
    val idCol = "id"
    val vectorizedCol = "vectorized"

    val df = parsed.toDF(idCol, textCol)
    val tokenizer = new RegexTokenizer()
      .setInputCol(textCol)
      .setOutputCol(rawTokensCol)
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(rawTokensCol)
      .setOutputCol(tokensCol)
    val countVectorizer = new CountVectorizer()
      .setVocabSize(vocabSize)
      .setInputCol(tokensCol)
      .setOutputCol(vectorizedCol)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))
    val model = pipeline.fit(df)

    val vectorized = model
      .transform(df)
      .select(idCol, vectorizedCol)
      .rdd
      .map({
        case Row(id: Integer, text: SparseVector)
        => id.toString + ';' + text
      })

    (vectorized, model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary)
  }
}

object ArticleParser {

  object Args {
    // Declared as var because JCommander assigns a new collection declared
    // as java.util.List because that's what JCommander will replace it with.
    // It'd be nice if JCommander would just use the provided List so this
    // could be a val and a Scala LinkedList.
    @Parameter(names = Array("-input"), description = "Input data dir", required = true)
    var input: String = null
    @Parameter(names = Array("-output"), description = "Output data dir", required = true)
    var output: String = null
    @Parameter(names = Array("--help"), help = true)
    var help: Boolean = false
  }

  def main(args: Array[String]) {
    val jc = new JCommander(Args, args.toArray: _*)
    if (Args.help) {
      jc.usage
      System.exit(0)
    }
    new ArticleParser(Args.input, Args.output).run()
  }

}