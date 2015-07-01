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
        var cores:Int = 28
        if (args.size > 0) {
            cores = args(0).toInt
        }

        // setup the Spark Context
        val sparkConf = new SparkConf().setAppName("FindFirstDegreeNeighbors")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.registerKryoClasses(Array(classOf[HBaseConfiguration], classOf[HTable], classOf[ByteBuffer], classOf[Put], classOf[Bytes]))
        val sc = new SparkContext(sparkConf)

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
        }.distinct().repartition(cores*4) // x4 cores

        // Save to HDFS
        "hdfs dfs -rm -r -f /data/first-degree-neighbors" !
        def makeStringRecord(record: (String, Array[Long])): String = {
            val sortedNeighbors = record._2.sortWith(sortByDecreasingPageRank)
            val neighborsString = sortedNeighbors.mkString(",")
            record._1+"\t"+neighborsString
        }
        neighborsByVertexId.map(makeStringRecord).saveAsTextFile(firstDegreeFiles)

        def putInHBase(vertex: (String, Array[Long])): Unit = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val vertexId = md5(vertex._1).toString
            // Row key is md5 hash of URL (vertexId)
            val putter = new Put(Bytes.toBytes(vertexId))
            // All 1st degree neighbors, sorted by decreasing Page Rank
            val sortedNeighbors = vertex._2.sortWith(sortByDecreasingPageRank)
            val neighborsFamilyName = Bytes.toBytes("Neighbors")
            val firstDegreeQualifierName = Bytes.toBytes("FirstDegree")
            val firstDegreeValue = Bytes.toBytes(sortedNeighbors.mkString(","))
            putter.addColumn(neighborsFamilyName, firstDegreeQualifierName, firstDegreeValue)
            table.put(putter)
            table.close()
        }

        def sortByDecreasingPageRank(vertexId1:Long, vertexId2:Long):Boolean = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val dataFamilyName = Bytes.toBytes("Data")
            val pageRankQualifierName = Bytes.toBytes("PageRank")

            val getter1 = new Get(Bytes.toBytes(vertexId1.toString))
            getter1.addColumn(dataFamilyName, pageRankQualifierName)
            val pageRank1 = table.get(getter1).value()

            val getter2 = new Get(Bytes.toBytes(vertexId2.toString))
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

        //Console.print(neighborsByVertexId.mapPartitions{ partitions => 
        //    for (row <- partitions) {
        //        putInHBase(row)
        //    }
        //    partitions
        //}.count())
  
    }
}
