name := "mesh"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
    "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    "org.apache.hbase" % "hbase" % "1.1.0.1",
    "org.apache.hbase" % "hbase-client" % "1.1.0.1",
    "org.apache.hbase" % "hbase-common" % "1.1.0.1"
)

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.0"
libraryDependencies += "com.esotericsoftware" % "kryo" % "3.0.1"
libraryDependencies += "com.martinkl.warc" % "warc-hadoop" % "0.1.0"

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
