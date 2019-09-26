import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, MinHashLSH}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}