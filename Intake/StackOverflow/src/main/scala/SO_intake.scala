import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object stack_intake {

	def main(args: Array[String]) {
		val url = "jdbc:postgresql://10.0.0.27:5432/postgres"
		val comment_path = "s3a://test-insight-data-pipes/stack/Comments.xml"
		val posts_path = "s3a://test-insight-data-pipes/stack/Posts.xml"

		val regex = "(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s"

		val spark = SparkSession.builder().appName("Copy Chat").getOrCreate()
  		import spark.implicits._

  		val stack_comments = spark.read.format("com.databricks.spark.xml").option("rowTag", "comments").load(comment_path)

  		val xml_rip = stack_comments.selectExpr("explode(row) as row")

  		val xml_rip2 = xml_rip.select("row.*", "*")

  		val xml_rip3 = xml_rip2.drop($"row")

  		val xml_rip4 = xml_rip3.drop("_UserDisplayName", "_VALUE", "_Score")


  		val sentences = xml_rip4.select($"_UserId" as "by", $"_CreationDate" as "time", $"_Id" as "id", explode(split($"_Text", regex)).as("sentence"))

  		val words = sentences.select($"by", $"time", $"id", split($"sentences", " ").as("words")))

		words.drop($"sentences")

  		words.write
  		.format("jdbc")
  		.option("url", url)
  		.option("dbtable", "stack")
  		.option("user", "postgres")
  		.option("password", "L0ngfins")
  		.option("driver", "org.postgresql.Driver")
  		.mode("overwrite")
  		.save()

  		val stack_posts = spark.read.format("com.databricks.spark.xml").option("rowTag", "posts").load(posts_path)

  		val dos_xml_rip = stack_posts.selectExpr("explode(row) as row")

  		val dos_xml_rip2 = dos_xml_rip.select("row.*", "*")

  		val dos_xml_rip3 = dos_xml_rip2.drop($"row")

  		val dos_xml_rip4 = dos_xml_rip3.drop("_UserDisplayName", "_VALUE", "_Score")

  		val dos_sentences = dos_xml_rip4.select($"_OwnerUserId" as "by", $"_CreationDate" as "time", $"_Id" as "id", explode(split($"_Body", regex)).as("sentence"))

  		val dos_words = sentences.select($"by", $"time", $"id", split($"sentences", " ").as("words")))

  		dos_words.write
  		.format("jdbc")
  		.option("url", url)
  		.option("dbtable", "stack")
  		.option("user", "postgres")
  		.option("password", "L0ngfins")
  		.option("driver", "org.postgresql.Driver")
  		.mode("append")
  		.save()

  		spark.stop()
  	}
  }
