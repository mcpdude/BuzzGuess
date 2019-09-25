import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, MinHashLSH}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

// import org.apache.spark.ml.linalg.Vectors




object HN_intake {

  def main(args: Array[String]) {

  	// Paths for the intake data and database
  	val url = "jdbc:postgresql://10.0.0.27:5432/postgres"
  	val path = "s3a://test-insight-data-pipes/hacker_news/"


  	// Initializing the tokenizer to split sentences into words later
  	val tokenizer = new Tokenizer().setInputCol("clean_sentences").setOutputCol("words")

  	// The regex for cleaning comments into sentences
  	val regex = "(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s"

  	// Start of the program
  	val spark = SparkSession.builder().appName("Copy Chat").getOrCreate()
  	import spark.implicits._

  	// Read in the data
    val HNdata = spark.read.option("inferSchema", true).json(path)
    // Filter to only look at comments
    val comments = HNdata.select("by", "time", "text", "id").filter($"type".like("comment"))
    // Split comments into sentences
    val sentences = comments.select($"by", $"time", $"id", explode(split($"text", regex)).as("sentence"))
    //sentences.printSchema()

    // Clean the sentences. There were several attempts at this, hence 'nice5'
    val nice = sentences.withColumn("clean_sentences", regexp_replace(sentences("sentence"), "\u0000", "0"))
    val nice1 = nice.withColumn("clean_sentences", regexp_replace(nice("clean_sentences"), "\\x00", "0"))
    val nice5 = nice1.drop("text").drop("sentence")

    // Celebratory debug message! 
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

  	val hashable = cvModel.transform(words_alone)

    val mh = new MinHashLSH().setNumHashTables(5).setInputCol("features").setOutputCol("sent_hash")

    val hashed = mh.fit(hashable)
    

    println("\n\n\n\n\n\n\n\n\n\nhere!")
    val finale = hashed.transform(hashable)
    println("\n\n\n\n\n\n\n\n\n\nhere!")


    // sentences.write.format("jdbc").option("url", url).option("dbtable", "sentences").option("user", "postgres").mode("overwrite").option("driver", "org.postgresql.Driver").option("password", "L0ngfins").save()
    words_alone.write.format("jdbc").option("url", url).option("dbtable", "yay!").option("user", "postgres").mode("overwrite").option("driver", "org.postgresql.Driver").option("password", "L0ngfins").save()
    
    spark.stop()
  }
}

