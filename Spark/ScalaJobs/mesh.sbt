name := "mesh"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
    "org.apache.hbase" % "hbase" % "0.99.2",
    "org.apache.hbase" % "hbase-client" % "0.99.2"
)

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.0"
