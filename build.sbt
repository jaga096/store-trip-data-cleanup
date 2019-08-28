name := "store-trip-data-cleanup"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

resolvers ++= Seq("maven-central" at "http://central.maven.org/maven2/")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0",
  "com.typesafe" % "config" % "1.3.1",
  "com.twitter" % "jsr166e" % "1.1.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "ch.qos.logback" % "logback-classic" % "1.1.7"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}