name := "netflixDataset"

version := "0.1"

scalaVersion := "2.13.10"
val sparkVersion = "3.0.1"
val hadoopVersion = "3.3.0"



libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.10" % sparkVersion
)
