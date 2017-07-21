name := "omid-dsl"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies += "com.lihaoyi" % "ammonite" % "1.0.0-RC9" % "test" cross CrossVersion.full

sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

// Optional, required for the `src` command to work
(fullClasspath in Test) ++= {
  (updateClassifiers in Test).value
    .configurations
    .find(_.configuration == Test.name)
    .get
    .modules
    .flatMap(_.artifacts)
    .collect{case (a, f) if a.classifier == Some("sources") => f}
}

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"

libraryDependencies += "org.apache.omid" % "omid-hbase-client" % "0.8.2.0"
libraryDependencies += "org.apache.omid" % "omid-hbase-tools" % "0.8.2.0" % "test"
libraryDependencies += "org.apache.omid" % "omid-tso-server" % "0.8.2.0" % "test"
libraryDependencies += "org.apache.omid" % "omid-metrics" % "0.8.2.0" % "test"
libraryDependencies += "org.apache.omid" % "omid-hbase0-shims" % "0.8.2.0" % "test"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-test" % "1.2.1" % "test"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "0.98.10.1-hadoop1"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.98.10.1-hadoop1"
libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "0.98.10.1-hadoop1" % "test" classifier "tests"
libraryDependencies += "org.apache.hbase" % "hbase-hadoop1-compat" % "0.98.10.1-hadoop1" % "test" classifier "tests"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.98.10.1-hadoop1" % "test" classifier "tests"
libraryDependencies += "org.apache.hbase" % "hbase-testing-util" % "0.98.10.1-hadoop1" % "test" classifier "tests"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "0.98.10.1-hadoop1" % "test" classifier "tests"
libraryDependencies += "com.google.inject" % "guice" % "3.0"


dependencyOverrides ++= Set("com.google.guava" % "guava" % "14.0.1")
