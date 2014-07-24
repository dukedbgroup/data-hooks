import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object DataHooksBuild extends Build {

 lazy val root = Project(id = "DataHooks", base = file("."), settings = rootSettings) 

 def sharedSettings = Defaults.defaultSettings ++ Seq(
   version := "0.1",
   scalaVersion := "2.9.3",
   scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
   unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
   
//   resolvers += "spray repo" at "http://repo.spray.io",

   libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
   )
 )

 def rootSettings = sharedSettings ++ Seq(
  publish := {}
 )

 def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

}
