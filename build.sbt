ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "food-clustering-datamart",
    
    libraryDependencies ++= Seq(
      // ClickHouse
      "com.clickhouse" % "clickhouse-jdbc" % "0.5.0",
      "com.clickhouse" % "clickhouse-client" % "0.5.0",
      
      // MySQL
      "mysql" % "mysql-connector-java" % "8.0.33",
      
      // HTTP client for API
      "com.softwaremill.sttp.client3" %% "core" % "3.8.15",
      "com.softwaremill.sttp.client3" %% "circe" % "3.8.15",
      
      // JSON processing
      "io.circe" %% "circe-core" % "0.14.5",
      "io.circe" %% "circe-generic" % "0.14.5",
      "io.circe" %% "circe-parser" % "0.14.5",
      
      // Configuration
      "com.typesafe" % "config" % "1.4.2",
      
      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.7",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "org.slf4j" % "slf4j-api" % "2.0.7",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      
      // Utilities
      "org.apache.commons" % "commons-csv" % "1.10.0"
    ),
    
    // Assembly plugin for fat JAR
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    
    assembly / assemblyJarName := "food-clustering-datamart.jar"
  )
