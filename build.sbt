lazy val root = (project in file(".")).
  settings(
  scalaVersion := "2.11.8",
  mainClass in assembly := Some("rtejada.projects.AREA.Main"),
  assemblyJarName in assembly := "AREA_2.0.jar",
  test in assembly := {}
  )
  
resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.2" withSources() withJavadoc()
libraryDependencies += "joda-time" % "joda-time" % "2.9.4" withJavadoc()
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.92-R10" withSources() withJavadoc()
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6+" withSources() withJavadoc()
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2" % "provided"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2" % "provided"
libraryDependencies += "org.codehaus.janino" % "janino" % "3.0.7"

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
