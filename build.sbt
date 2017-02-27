lazy val root = (project in file(".")).
  settings(
  name := "AREA",
  version := "2.0",
  scalaVersion := "2.11.8",
  mainClass in Compile := Some("rtejada.projects.AREA.Main") 
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"  withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.2" withSources() withJavadoc()
libraryDependencies += "joda-time" % "joda-time" % "2.9.4" withJavadoc()
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "provided" 
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.92-R10" withSources() withJavadoc()
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6+" withSources() withJavadoc()
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.9"

EclipseKeys.withSource := true
EclipseKeys.withJavadoc := true

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// put all libs in the lib_managed directory, that way we can distribute eclipse project files
retrieveManaged := true
    
EclipseKeys.relativizeLibs := true
