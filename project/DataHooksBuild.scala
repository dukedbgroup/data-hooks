import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object DataHooksBuild extends Build {

 // Hadoop version to build against.
 val HADOOP_VERSION = "2.6.0"

 // Spark version to build againt.
 val SPARK_VERSION = "1.0.1"

 // Hive version
 val HIVE_VERSION = "1.2.0"

 lazy val root = Project(id = "DataHooks", base = file("."), settings = rootSettings) 

 def sharedSettings = Defaults.defaultSettings ++ Seq(
   version := "0.1",
   scalaVersion := "2.10.3",
   scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
   unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
   
//   resolvers += "spray repo" at "http://repo.spray.io",
	resolvers ++= Seq(
			"conjars.org" at "http://conjars.org/repo"
		),

   libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
   )
 )

 def rootSettings = assemblySettings ++ sharedSettings ++ Seq(
  name := "datahooks",
  libraryDependencies ++= Seq(
      "org.antlr" % "antlr-runtime" % "3.4" % "provided",
      "log4j" % "log4j" % "1.2.16" % "provided",
      "commons-logging" % "commons-logging" % "1.1.1" % "provided",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.3.3" % "provided",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.3.3" % "provided",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3" % "provided",
      "junit" % "junit" % "4.11" % "provided",
      "mysql" % "mysql-connector-java" % "5.1.35",
      "org.apache.spark" % "spark-core_2.10" % SPARK_VERSION % "provided",
      "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION % "provided",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HADOOP_VERSION % "provided",
      // "org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION % "provided",
      "org.apache.hive" % "hive-exec" % HIVE_VERSION % "provided",
      "org.apache.hive" % "hive-metastore" % HIVE_VERSION % "provided",
      "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.0",
      "org.codehaus.jackson" % "jackson-core-asl" % "1.9.0",
      "org.apache.httpcomponents" % "httpclient" % "4.3.6",
      "com.timgroup" % "java-statsd-client" % "3.1.0"
  ),
  publish := {}
 ) ++ extraAssemblySettings

 def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

}
