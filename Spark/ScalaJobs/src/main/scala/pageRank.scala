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

        val hdfsPath = "hdfs://"+sys.env("HADOOP_IP")+":9000"
        val warcFileEdges = hdfsPath+"/data/link-edges"
        val edgeListFiles = hdfsPath+"/data/edge-lists"
        val vertexIdFiles = hdfsPath+"/data/vertex-ids"

        def md5(s: String): Int = {
            val message = MessageDigest.getInstance("MD5").digest(s.getBytes)
            ByteBuffer.wrap(message).getInt
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
        // RDD[record:String]
        val rdd = sc.textFile(warcFileEdges)

        // map each VertexName to its VertexId
        // RDD[(Long, String)]
        //val vertices = rdd.map(mapVertexHash).reduceByKey((a, b) => a) // Removes duplicates
        //Console.print(vertices.take(10).mkString("\n") + "\n")
        // RDD[record:(Long, String)]
        val vertices = sc.textFile(vertexIdFiles).map { line =>
            val fields = line.split(" ")
            (fields(0).toLong, fields(1))
        }.distinct()

        // Setup GraphX graph
        val graph = GraphLoader.edgeListFile(sc, edgeListFiles)
        // Run PageRank
        val ranks = graph.pageRank(0.0001).vertices

        // Map VertexIds to URL
        // RDD[(url:String, pageRank:Double)]
        val ranksByVertexUrl = vertices.join(ranks).map {
            case (id, (vid, rank)) => (vid, rank)
        }.distinct()

        Console.print(ranksByVertexUrl.take(10).mkString("\n") + "\n")

        // Store ranks to HBase
        def putInHBase(vertex: (String, Double)): Unit = {
            val hbaseConf = HBaseConfiguration.create()
            hbaseConf.set("hbase.zookeeper.quorum", "ec2-52-8-87-99.us-west-1.compute.amazonaws.com")
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val vertexId = md5(vertex._1).toString
            // Row key is md5 hash of URL (vertexId)
            val putter = new Put(Bytes.toBytes(vertexId))
            val dataFamilyName = Bytes.toBytes("Data")
            val vertexIdQualifierName = Bytes.toBytes("VertexId")
            val vertexIdValue = Bytes.toBytes(vertexId)
            putter.addColumn(dataFamilyName, vertexIdQualifierName, vertexIdValue)
            val urlQualifierName = Bytes.toBytes("URL")
            val urlValue = Bytes.toBytes(vertex._1)
            putter.addColumn(dataFamilyName, urlQualifierName, urlValue)
            val pageRankQualifierName = Bytes.toBytes("PageRank")
            val pageRankValue = Bytes.toBytes(vertex._2.toString)
            putter.addColumn(dataFamilyName, pageRankQualifierName, pageRankValue)
            table.put(putter)
            table.close()
        }

        Console.print(ranksByVertexUrl.map(putInHBase).count())
    }
}
