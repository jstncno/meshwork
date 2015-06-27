import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal

import java.security.MessageDigest
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}

object secondDegreeNeighbors {
    def main(args: Array[String]) {

        // setup the Spark Context
        val sparkConf = new SparkConf().setAppName("FindSecondDegreeNeighbors")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.registerKryoClasses(Array(classOf[HBaseConfiguration], classOf[HTable], classOf[ByteBuffer], classOf[Get], classOf[Put], classOf[Bytes]))
        val sc = new SparkContext(sparkConf)

        val hdfsPath = "hdfs://"+sys.env("HADOOP_IP")+":9000"
        val warcFileEdges = hdfsPath+"/data/link-edges"
        val edgeListFiles = hdfsPath+"/data/edge-lists"
        val vertexIdFiles = hdfsPath+"/data/vertex-ids"
        val firstDegreeFiles = hdfsPath+"/data/first-degree-neighbors"

        def md5(s: String): Int = {
            val message = MessageDigest.getInstance("MD5").digest(s.getBytes)
            ByteBuffer.wrap(message).getInt
        }

        // RDD[(String, Set[String])]
        val vertices = sc.textFile(vertexIdFiles).map { line =>
            val fields = line.split(" ")
            (fields(0).toLong, fields(1))
        }.distinct().repartition(80) // 4 x 20 cores

        // Find second-degree neighbors of each vertex
        // Neighbers represented as Set[VertexId]
        def getSecondDegreeNeighbors(vertex: (Long, String)): (String, Set[String]) = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val vertexId = vertex._1.toString
            val getter = new Get(Bytes.toBytes(vertexId))
            val neighborsFamilyName = Bytes.toBytes("Neighbors")
            val firstDegreeQualifierName = Bytes.toBytes("FirstDegree")
            getter.addColumn(neighborsFamilyName, firstDegreeQualifierName)
            val results = table.get(getter).value()
            table.close()
            if (results != null) {
                var secondDegreeNeighbors = Set[String]()
                val firstDegreeNeighbors = new String(results).split(",").toSet
                for (neighbor <- firstDegreeNeighbors) {
                    // neighbor is a vertexId
                    val neighborGetter = new Get(Bytes.toBytes(neighbor))
                    neighborGetter.addColumn(neighborsFamilyName, firstDegreeQualifierName)
                    val r = table.get(neighborGetter).value()
                    if (r != null) {
                        secondDegreeNeighbors ++= new String(r).split(",").toSet
                    }
                }
                (vertex._2, secondDegreeNeighbors)
            } else {
                (vertex._2, Set[String]())
            }
        }

        val neighbors = vertices.mapPartitions { partitions =>
            var res = Array[(String, Set[String])]()
            for (record <- partitions) {
                res :+= getSecondDegreeNeighbors(record)
            }
            res.iterator
        }

        def putInHBase(vertex: (String, Set[String])): Unit = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val vertexId = md5(vertex._1).toString
            // Row key is md5 hash of URL (vertexId)
            val putter = new Put(Bytes.toBytes(vertexId))
            // All 2nd degree neighbors, sorted by decreasing Page Rank
            val sortedNeighbors = vertex._2.toArray.sortWith(sortByDecreasingPageRank)
            val neighborsFamilyName = Bytes.toBytes("Neighbors")
            val secondDegreeQualifierName = Bytes.toBytes("SecondDegree")
            val secondDegreeValue = Bytes.toBytes(sortedNeighbors.mkString(","))
            putter.addColumn(neighborsFamilyName, secondDegreeQualifierName, secondDegreeValue)
            table.put(putter)
            table.close()
        }

        def sortByDecreasingPageRank(vertexId1:String, vertexId2:String):Boolean = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val dataFamilyName = Bytes.toBytes("Data")
            val pageRankQualifierName = Bytes.toBytes("PageRank")

            val getter1 = new Get(Bytes.toBytes(vertexId1))
            getter1.addColumn(dataFamilyName, pageRankQualifierName)
            val pageRank1 = table.get(getter1).value()

            val getter2 = new Get(Bytes.toBytes(vertexId2))
            getter2.addColumn(dataFamilyName, pageRankQualifierName)
            val pageRank2 = table.get(getter2).value()

            table.close()
            if (pageRank1 == null) {
                return false
            } else if (pageRank2 == null) {
                return true
            }
            return new String(pageRank1).toDouble > new String(pageRank2).toDouble
        }

        Console.print(neighbors.mapPartitions{ partitions =>
            for (record <- partitions) {
                putInHBase(record)
            }
            partitions
        }.count())
    }
}
