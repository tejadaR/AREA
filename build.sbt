lazy val root = (project in file(".")).
  settings(
  name := "AREA",
  version := "2.0",
  scalaVersion := "2.11.8",
  mainClass in Compile := Some("rtejada.projects.AREA.Main") 
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.2" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.2" withSources() withJavadoc()
libraryDependencies += "joda-time" % "joda-time" % "2.9.4" withJavadoc()
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.92-R10"  withSources() withJavadoc()
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6+" withSources() withJavadoc()
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.9"

EclipseKeys.withSource := true
EclipseKeys.withJavadoc := true

// META-INF discarding
assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
