import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, MinHashLSH}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

// import org.apache.spark.ml.linalg.Vectors




object HN_intake {

  def main(args: Array[String]) {
  	val url = "jdbc:postgresql://10.0.0.25:5432/postgres"
  	val path = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/hn/"

  	val tokenizer = new Tokenizer().setInputCol("clean_sentences").setOutputCol("words")



  	// val conf = new SparkConf().setAppName("Simple Application")
  	// val sc = new SparkContext(conf)

  	val regex = "(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s"
  	val spark = SparkSession.builder().appName("Copy Chat").getOrCreate()
  	import spark.implicits._

    val HNdata = spark.read.option("inferSchema", true).json(path)
    val comments = HNdata.select("by", "time", "text", "id").filter($"type".like("comment"))
    val sentences = comments.select($"by", $"time", $"id", explode(split($"text", regex)).as("sentence"))
    sentences.printSchema()
    val nice = sentences.withColumn("clean_sentences", regexp_replace(sentences("sentence"), "\u0000", "0"))
    val nice1 = nice.withColumn("clean_sentences", regexp_replace(nice("clean_sentences"), "\\x00", "0"))
    val nice5 = nice1.drop("text").drop("sentence")
    nice5.printSchema()
    println("it's working!")

    val words =  nice5.select($"by", $"time", $"id", split($"clean_sentences", " ").as("words"))
    val words_alone = words.drop("clean_sentences")

    words_alone.printSchema()

    val cvModel: CountVectorizerModel = new CountVectorizer()
  	.setInputCol("words")
  	.setOutputCol("features")
  	.setVocabSize(3000)
  	.setMinDF(2)
  	.fit(words_alone)

  	cvModel.transform(words_alone).show(true)

    val mh = new MinHashLSH().setNumHashTables(5).setInputCol("features").setOutputCol("sent_hash")

    val with_hash = mh.fit(words_alone)
    words_alone.printSchema()

    println("\n\n\n\n\n\n\n\n\n\nhere!")
    with_hash.transform(words_alone).show()
    println("\n\n\n\n\n\n\n\n\n\nhere!")

    words_alone.printSchema()

    // sentences.write.format("jdbc").option("url", url).option("dbtable", "sentences").option("user", "postgres").mode("overwrite").option("driver", "org.postgresql.Driver").option("password", "L0ngfins").save()
    words_alone.write.format("jdbc").option("url", url).option("dbtable", "yay!").option("user", "postgres").mode("overwrite").option("driver", "org.postgresql.Driver").option("password", "L0ngfins").save()
    
    spark.stop()
  }
}

