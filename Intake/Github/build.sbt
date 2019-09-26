name := "Github intake"

version := "1.0"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.4.0",
	"org.apache.spark" %% "spark-sql" % "2.4.0",
	"com.databricks" %% "spark-xml" % "0.6.0"
)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
