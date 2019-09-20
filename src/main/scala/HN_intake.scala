import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
// import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
// import org.apache.spark.ml.linalg.Vectors



object HN_intake {

  def main(args: Array[String]) {
  	val url = "jdbc:postgresql://10.0.0.25:5432/postgres"
  	val path = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/hn/"
  	val regex = "(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s"
  	val spark = SparkSession.builder().appName("Copy Chat").getOrCreate()
  	import spark.implicits._

    val HNdata = spark.read.option("inferSchema", true).json(path)
    val comments = HNdata.select("by", "time", "text", "id").filter($"type".like("comment"))
    val sentences = comments.select($"by", $"time", $"id", explode(split($"text", regex)).as("sentence"))
    sentences.printSchema()
    val nice = sentences.withColumn("clean_sentences", regexp_replace(sentences("sentence"), "\u0000", "0"))
    val nice1 = nice.withColumn("clean_sentences", regexp_replace(nice("clean_sentences"), "\\x00", "0"))
    val nice2 = nice1.withColumn("clean_sentences", regexp_replace(nice1("clean_sentences"), "\\\\0x00", "0"))
    // val nice3 = nice2.withColumn("clean_sentences", regexp_replace(nice2("clean_sentences"), "0x00", "0"))
    nice3.printSchema()
    val nice5 = nice2.drop("text").drop("sentence")
    nice5.printSchema()
    println("it's working!")

    // val mh = new MinHashLSH().setNumHashTables(5).setInputCol("clean_sentences").setOututCol("sent _hash")

    // val with_hash = mh.fit(nice5)

    // with_hash.transform(nice5).show()

    // sentences.write.format("jdbc").option("url", url).option("dbtable", "sentences").option("user", "postgres").mode("overwrite").option("driver", "org.postgresql.Driver").option("password", "L0ngfins").save()
    nice5.write.format("jdbc").option("url", url).option("dbtable", "nice").option("user", "postgres").mode("overwrite").option("driver", "org.postgresql.Driver").option("password", "L0ngfins").save()
    
    spark.stop()
  }
}

