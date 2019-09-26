import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, MinHashLSH, CountVectorizer, CountVectorizerModel}

object Process {

	def main(args: Array[String]) {
		val url = "jdbc:postgresql://10.0.0.27:5432/postgres"
		val stack_query = "(SELECT * from stack) as foo"
		val hn_query = "(SELECT * from yay) as foo"


		val spark = SparkSession.builder().appName("Processing Data").getOrCreate()
  		import spark.implicits._


		val stack_data = spark.read
						.format("jdbc")
						.option("url", url)
            			.option("dbtable", stack_query)
            			.option("user", "postgres")
            			.option("password", "L0ngfins")
            			.option("driver", "org.postgresql.Driver").load()

		val hn_data = spark.read
						.format("jdbc")
						.option("url", url)
            			.option("dbtable", hn_query)
            			.option("user", "postgres")
            			.option("password", "L0ngfins")
            			.option("driver", "org.postgresql.Driver").load()

        stack_data.printSchema()
        hn_data.printSchema()

        val big_table = hn_data.union(stack_data)

        big_table.printSchema()

	    val cvModel: CountVectorizerModel = new CountVectorizer()
	    	.setInputCol("words")
	    	.setOutputCol("features")
	    	.setVocabSize(3000)
	    	.setMinDF(2)
	    	.fit(big_table)


	    val hashable = cvModel.transform(big_table)