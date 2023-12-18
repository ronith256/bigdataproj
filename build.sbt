val scalaVer = "2.12.18"

lazy val root = project
  .in(file("."))
  .settings(
    name := "bigdataproj",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scalaVer,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-mllib" % "3.5.0",
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.11.0",
      "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
      "mysql" % "mysql-connector-java" % "8.0.23"
    )
  )