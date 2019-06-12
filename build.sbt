name := "Consume IoT Azure by Spark - classic streaming"

version := "0.0.1"

scalaVersion := "2.11.0"

resolvers += "mapr-releases" at "http://repository.mapr.com/maven/"

resolvers += "thirdparty-releases" at "https://repository.jboss.org/nexus/content/repositories/thirdparty-releases/"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0.SP5"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1-mapr-1803" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1-mapr-1803" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.1-mapr-1803"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.1"

libraryDependencies += "org.ojai" % "ojai-scala" % "2.0.1-mapr-1804"

libraryDependencies += "com.mapr.db" % "maprdb-spark" % "2.2.1-mapr-1803"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0-RC1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1"
