name := "Udf-example"
version := "1.0"
scalaVersion := "2.12.10"
val sparkVersion = "3.0.0-preview2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion )  
