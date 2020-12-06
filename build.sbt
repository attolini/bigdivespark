name := "bigdivespark"

version := "0.0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

resolvers := List(
  "Hortonworks Repository" at "https://repo.hortonworks.com/content/repositories/releases/",
  "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
