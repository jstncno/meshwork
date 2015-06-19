import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal

import org.apache.spark.serializer.KryoSerializer

import java.security.MessageDigest
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}

object pageRank {
    def main(args: Array[String]) {

        // setup the Spark Context
        val sparkConf = new SparkConf().setAppName("RunPageRank")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.registerKryoClasses(Array(classOf[HBaseConfiguration], classOf[HTable], classOf[ByteBuffer], classOf[Put], classOf[Bytes]))
        val sc = new SparkContext(sparkConf)

        val warcFileEdges = "hdfs://ip-172-31-10-101:9000/data/link-edges"
        val edgeListFiles = "hdfs://ip-172-31-10-101:9000/data/edge-lists"

        def md5(s: String): Long = {
            val message = MessageDigest.getInstance("MD5").digest(s.getBytes)
            ByteBuffer.wrap(message).getLong
        }

        // function to map src_url to its hash integer
        def mapVertexHash(record: String): (Long, String) = {
            val error = md5("error")
            val r = record.split(", ")
            // Catch ArrayIndexOutOfBoundsException
            try {
                val src_url = r(0).replace("(", "")
                (md5(src_url), src_url)
            } catch {
                case NonFatal(exc) => (error, "error")
            }
        }

        // read in the data from HDFS
        // RDD[record:String]
        val rdd = sc.textFile(warcFileEdges)

        // map each VertexName to its VertexId
        // RDD[(Long, String)]
        val vertices = rdd.map(mapVertexHash).reduceByKey((a, b) => a) // Removes duplicates
        Console.print(vertices.take(10).mkString("\n") + "\n")

        // Setup GraphX graph
        val graph = GraphLoader.edgeListFile(sc, edgeListFiles)
        // Run PageRank
        val ranks = graph.pageRank(0.0001).vertices

        // Map VertexIds to URL
        // RDD[(url:Long, pageRank:Double)]
        val ranksByVertexUrl = vertices.join(ranks).map {
            case (id, (vid, rank)) => (vid, rank)
        }

        Console.print(ranksByVertexUrl.take(10).mkString("\n") + "\n")

        // Store ranks to HBase
        def putInHBase(vertex: (String, Double)): Unit = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            // Row key is md5 hash of URL
            val vertexId = md5(vertex._1)
            val putter = new Put(vertexId)
            val dataFamilyName = Bytes.toBytes("Data")
            val urlQualifierName = Bytes.toBytes("URL")
            val urlValue = Bytes.toBytes(vertex._1)
            putter.addColumn(dataFamilyName, urlQualifierName, urlValue)
            val pageRankQualifierName = Bytes.toBytes("PageRank")
            val pageRankValue = vertex._2
            putter.addColumn(dataFamilyName, pageRankQualifierName, pageRankValue)
            table.put(putter)
        }

        Console.print(ranksByVertexUrl.map(putInHBase).count())
    }
}
