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

        val vertices = sc.textFile(vertexIdFiles).map { line =>
            val fields = line.split(" ")
            (fields(0).toLong, fields(1))
        }.distinct()

        // Find second-degree neighbors of each vertex
        // Neighbers represented as Set[VertexId]
        def getSecondDegreeNeighbors(vertex: (Long, String)) = {
            val hbaseConf = HBaseConfiguration.create()
            hbaseConf.set("hbase.zookeeper.quorum", "ec2-52-8-87-99.us-west-1.compute.amazonaws.com")
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val vertexId = vertex._1.toString
            val getter = new Get(Bytes.toBytes(vertexId))
            val neighborsFamilyName = Bytes.toBytes("Neighbors")
            val firstDegreeQualifierName = Bytes.toBytes("FirstDegree")
            getter.addColumn(neighborsFamilyName, firstDegreeQualifierName)
            val results = new String(table.get(getter).value())
            results.split(",").toSet
        }

        //val neighbors = vertices.map(getSecondDegreeNeighbors).reduce(_ ++ _)
        val neighbors = vertices.map(getSecondDegreeNeighbors)
        Console.print(neighbors.take(10))

        /*
        // Map VertexIds to URL
        val neighborsByVertexId = vertices.join(neighbors).map {
            case (id, (vid, n)) => (vid, n)
        }.distinct()

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

        Console.print(neighborsByVertexId.map(putInHBase).count())
        */
    }
}
