name := """storm-scala"""

version := "1.0"

scalaVersion := "2.11.5"

resolvers += "clojars" at "https://clojars.org/repo"

libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "0.9.5" % "provided" exclude("junit", "junit"),
  "com.hazelcast" % "hazelcast" % "3.4.4",
  "com.hazelcast" % "hazelcast-client" % "3.4.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
)

fork in Test := true

// Topology integration test takes more time that the default 5 seconds timeout
envVars ++= Map("STORM_TEST_TIMEOUT_MS" -> "60000")

parallelExecution in Test := false

fork in run := true