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
            ec.sendToSrc(Array(ec.dstId))
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
        val neighborsByVertexId = vertices.join(neighbors).map {
            case (id, (vid, n)) => (vid, n)
        }.distinct()

        // Save to HDFS
        neighborsByVertexId.map { record =>
            val neighborsString = record._2.mkString(",")
            record._1+","+neighborsString
        }.saveAsTextFile(firstDegreeFiles)

        def putInHBase(vertex: (String, Array[Long])): Unit = {
            val hbaseConf = HBaseConfiguration.create()
            hbaseConf.set("hbase.zookeeper.quorum", "ec2-52-8-87-99.us-west-1.compute.amazonaws.com")
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
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

        //Console.print(neighborsByVertexId.map(putInHBase).count())
    }
}
