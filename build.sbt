organization := "MapReduceLab"

version := "0.1"

name := "rustyhands"

scalaVersion := "2.11.12"

mainClass in Compile := Some("com.mapreducelab.spark.anomalies.Detector")

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" withSources() withJavadoc(),
  "com.typesafe" % "config" % "1.3.2", // I've used to this ex-typesafe library https://github.com/lightbend/config
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2", // https://github.com/lightbend/scala-logging
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}