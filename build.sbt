name := """storm-scala"""

version := "1.0"

scalaVersion := "2.11.5"

resolvers += "clojars" at "https://clojars.org/repo"

libraryDependencies ++= Seq(
    "org.apache.storm" % "storm-core" % "0.9.3" % "provided" exclude("junit", "junit"),
    "com.hazelcast" % "hazelcast" % "3.4.1",
    "com.hazelcast" % "hazelcast-client" % "3.4.1",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test")

fork in run := true