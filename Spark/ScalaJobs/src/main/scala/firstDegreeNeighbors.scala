import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal
import scala.sys.process._
import scala.util.Sorting

import java.security.MessageDigest
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}

object firstDegreeNeighbors {
    def main(args: Array[String]) {

        // setup the Spark Context
        val conf = new SparkConf().setAppName("FindFirstDegreeNeighbors")
        val sc = new SparkContext(conf)

        //val warcFileEdges = "hdfs://ip-172-31-10-101:9000/common-crawl/crawl-data/CC-MAIN-2015-18/segments/1429246633512.41/warc/warc-edges-00000"
        val hdfsPath = "hdfs://"+sys.env("HADOOP_IP")+":9000"
        val warcFileEdges = hdfsPath+"/data/link-edges"
        val edgeListFiles = hdfsPath+"/data/edge-lists"
        val vertexIdFiles = hdfsPath+"/data/vertex-ids"
        val firstDegreeFiles = hdfsPath+"/data/first-degree-neighbors"

        def md5(s: String): Int = {
            val message = MessageDigest.getInstance("MD5").digest(s.getBytes)
            ByteBuffer.wrap(message).getInt
        }

        // sendMsg function to send to all edges in graph
        def sendDstIdToSrc(ec:EdgeContext[Int, Int, Array[Long]]): Unit = {
            //ec.sendToSrc(Array(ec.dstId))
            if (ec.srcId != ec.dstId) {
                ec.sendToDst(Array(ec.srcId))
            }
        }

        // function to map src_url to its hash integer
        def mapVertexHash(record: String): (Long, String) = {
            val error = md5("error").toLong
            val r = record.split(" ")
            // Catch ArrayIndexOutOfBoundsException
            try {
                val src_url = r(0)
                (md5(src_url).toLong, src_url)
            } catch {
                case NonFatal(exc) => (error, "error")
            }
        }

        // read in the data from HDFS
        val rdd = sc.textFile(warcFileEdges)

        // map each VertexName to its VertexId
        //val vertices = rdd.map(mapVertexHash).reduceByKey((a, b) => a)
        val vertices = sc.textFile(vertexIdFiles).map { line =>
            val fields = line.split(" ")
            (fields(0).toLong, fields(1))
        }.distinct()

        // Setup GraphX graph
        val graph = GraphLoader.edgeListFile(sc, edgeListFiles)

        // Find first-degree neighbors of each vertex
        // Neighbers represented as Array[VertexId]
        val neighbors = graph.aggregateMessages[Array[Long]](sendDstIdToSrc, _ ++ _)

        // Map VertexIds to URL
        // (VertexIds: String, Neighbors: Array[Long])
        val neighborsByVertexId = vertices.join(neighbors).map {
            case (id, (vid, n)) => (vid, n)
        }.distinct().repartition(80) // 4 x 20 cores

        // Save to HDFS
        /*"hdfs dfs -rm -r -f /data/first-degree-neighbors" !
        neighborsByVertexId.map { record =>
            val neighborsString = record._2.mkString(",")
            record._1+","+neighborsString
        }.saveAsTextFile(firstDegreeFiles)*/

        def putInHBase(vertex: (String, Array[Long])): Unit = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val vertexId = md5(vertex._1).toString
            // Row key is md5 hash of URL (vertexId)
            val putter = new Put(Bytes.toBytes(vertexId))
            val neighborsFamilyName = Bytes.toBytes("Neighbors")
            val firstDegreeQualifierName = Bytes.toBytes("FirstDegree")
            val firstDegreeValue = Bytes.toBytes(vertex._2.mkString(","))
            putter.addColumn(neighborsFamilyName, firstDegreeQualifierName, firstDegreeValue)
            table.put(putter)
            table.close()
        }

        Console.print(neighborsByVertexId.mapPartitions{ partitions => 
            for (row <- partitions) {
                putInHBase(row)
            }
            partitions
        }.count())

        object PageRankOrdering extends Ordering[Long] {
            def compare(vertexId1:Long, vertexId2:Long) = {
                val hbaseConf = HBaseConfiguration.create()
                val tableName = "websites"
                val table = new HTable(hbaseConf, tableName)
                val dataFamilyName = Bytes.toBytes("Data")
                val pageRankQualifierName = Bytes.toBytes("PageRank")

                val getter1 = new Get(Bytes.toBytes(vertexId1))
                getter1.addColumn(dataFamilyName, pageRankQualifierName)
                val pageRank1 = table.get(getter1).value()

                val getter2 = new Get(Bytes.toBytes(vertexId2))
                getter1.addColumn(dataFamilyName, pageRankQualifierName)
                val pageRank2 = table.get(getter2).value()

                table.close()

                if (pageRank1 == null && pageRank2 == null) {
                    0 compare 0
                } else if (pageRank1 != null && pageRank2 == null) {
                    1 compare 0
                } else if (pageRank1 == null && pageRank2 != null) {
                    0 compare 1
                } else {
                    new String(pageRank2).toDouble compare new String(pageRank1).toDouble
                }
            }
        }

        // Top K neighbors (by page rank) of vertex
        def getTopKNeighbors(vertex: (String, Array[Long]), k: Int): (String, Array[Long]) = {
            Sorting.quickSort(vertex._2)(PageRankOrdering)
            (vertex._1, vertex._2.take(k))
        }

        // neighborsByVertexId
        // RDD[(VertexIds: String, Neighbors: Array[Long])]
        val top100Neighbors = neighborsByVertexId.mapPartitions{ partitions => 
            var res = Array[(String, Array[Long])]()
            for (row <- partitions) {
                res :+= getTopKNeighbors(row, 100)
            }
            res.iterator
        }
    }
}
