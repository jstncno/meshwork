import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal

import java.security.MessageDigest
import java.nio.ByteBuffer

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}

object pageRank {
    def main(args: Array[String]) {

        // setup the Spark Context
        val sparkConf = new SparkConf().setAppName("CreateEdgeListFile")
        val sc = new SparkContext(sparkConf)

        val warcFileEdges = "hdfs://ip-172-31-10-101:9000/common-crawl/crawl-data/CC-MAIN-2015-18/segments/1429246633512.41/warc/warc-edges-00000"
        val edgeListFiles = "hdfs://ip-172-31-10-101:9000/data/edge-lists"

        // function to map src_url to its hash integer
        def mapVertexHash(record: String): (Long, String) = {
            val error = "error".hashCode.toLong
            val r = record.split(", ")
            // Catch ArrayIndexOutOfBoundsException
            try {
                val src_url = r(0).replace("(", "")
                (src_url.hashCode.toLong, src_url)
            } catch {
                case NonFatal(exc) => (error, "error")
            }
        }

        // read in the data from HDFS
        val rdd = sc.textFile(warcFileEdges)

        // map each VertexName to its VertexId
        val vertices = rdd.map(mapVertexHash).reduceByKey((a, b) => a)

        // Setup GraphX graph
        val graph = GraphLoader.edgeListFile(sc, edgeListFiles)
        // Run PageRank
        val ranks = graph.pageRank(0.0001).vertices

        // Map VertexIds to URL
        val ranksByVertexId = vertices.join(ranks).map {
            case (id, (vid, rank)) => (vid, rank)
        }

        Console.print(ranksByVertexId.take(10).mkString("\n") + "\n")


        // Store ranks to HBase
        val tableName = "websites"
        val hbaseConf = HBaseConfiguration.create()
        val admin = new HBaseAdmin(hbaseConf)
        //if (!admin.isTableAvailable(tableName)) {
            //val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
            //admin.createTable(tableDesc)
        //}
        val table = new HTable(hbaseConf, tableName)

        def md5(s: String) = {
            MessageDigest.getInstance("MD5").digest(s.getBytes)
        }

        def putInHBase(vertex: (String, Double)): Unit = {
            // Row key is md5 hash of URL
            val vertexId = ByteBuffer.wrap(md5(vertex._1)).getLong
            val putter = new Put(Bytes.toBytes(vertexId))
            val dataFamilyName = Bytes.toBytes("Data")
            val urlQualifierName = Bytes.toBytes("URL")
            val urlValue = Bytes.toBytes(vertex._1)
            putter.addColumn(dataFamilyName, urlQualifierName, urlValue)
            val pageRankQualifierName = Bytes.toBytes("PageRank")
            val pageRankValue = Bytes.toBytes(vertex._2)
            putter.addColumn(dataFamilyName, pageRankQualifierName, pageRankValue)
            table.put(putter)
        }

        ranksByVertexId.map(putInHBase).count()
    }
}
