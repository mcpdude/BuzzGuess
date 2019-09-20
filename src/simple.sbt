name := "CopyChat App"

version := "1.0"

scalaVersion := "2.11.11"



libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.11"              % "1.1.0",
  "org.apache.spark"  % "spark-sql"					   % "2.2.0",
  "org.apache.spark"  %% "spark-ml"                  % "2.0.1"
  )