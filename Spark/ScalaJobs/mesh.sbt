name := "mesh"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
    "org.apache.hbase" % "hbase" % "1.1.0.1",
    "org.apache.hbase" % "hbase-client" % "1.1.0.1",
    "org.apache.hbase" % "hbase-common" % "1.1.0.1"
)

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.0"
