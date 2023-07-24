ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "NBA_DATA"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2"
)

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.8.0",
  "com.lihaoyi" %% "ujson" % "3.0.0",
  "com.lihaoyi" %% "os-lib" % "0.9.1",
)