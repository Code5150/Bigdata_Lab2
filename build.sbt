name := "bigdata_lab2_2"

version := "0.1"

scalaVersion := "2.12.14"

fork := true

idePackagePrefix := Some("com.vladislav")

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.14.0"
)