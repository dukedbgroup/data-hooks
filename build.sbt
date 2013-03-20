import AssemblyKeys._ // put this at the top of the file

assemblySettings

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "spark-core-assembly-0.7.0-SNAPSHOT.jar"}
}

name := "SparkQueriesPlanner"

version := "1.0"

scalaVersion := "2.9.2"

