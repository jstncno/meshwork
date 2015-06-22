import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal
import java.security.MessageDigest
import java.nio.ByteBuffer
import scala.sys.process._

import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}

object vertexIds {
    def main(args: Array[String]) {

        // setup the Spark Context
        val sparkConf = new SparkConf().setAppName("CreateVertexIds")
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.registerKryoClasses(Array(classOf[HBaseConfiguration], classOf[HTable], classOf[ByteBuffer], classOf[Put], classOf[Bytes]))
        val sc = new SparkContext(sparkConf)

        val hdfsPath = "hdfs://"+sys.env("MASTER_NAME")+":9000"
        val warcFileEdges = hdfsPath+"/data/link-edges"

        // md5 hash
        def md5(s: String): Int = {
            val message = MessageDigest.getInstance("MD5").digest(s.getBytes)
            ByteBuffer.wrap(message).getInt
        }

        // function to get vertex ID by md5 hashing URL
        def vertexIdHash(record: String): Array[String] = {
            val error = md5("error").toString
            val r = record.split(" ")
            // Catch ArrayIndexOutOfBoundsException
            try {
                val src_url = r(0)
                val dst_url = r(1)
                val src = md5(src_url).toString + " " + src_url
                val dst = md5(dst_url).toString + " " + dst_url
                Array(src, dst)
            } catch {
                case NonFatal(exc) => Array(error + " error")
            }
        }

        // read in the data from HDFS
        val rdd = sc.textFile(warcFileEdges)

        // map the src_url and dst_url of each record to their vertex Id
        val vertexIds = rdd.map(vertexIdHash).flatMap(record => record).distinct()

        // delete existing directory
        "hdfs dfs -rm -r -f /data/vertex-ids" !
        // save the data back into HDFS
        val vertexIdFileName = hdfsPath+"/data/vertex-ids"
        vertexIds.saveAsTextFile(vertexIdFileName)

        Console.print("Vertex ID file saved to /data/vertex-ids\n")

        // Store URLs and VertexIds in HBase
        def putInHBase(vertex: String): Unit = {
            val hbaseConf = HBaseConfiguration.create()
            val tableName = "websites"
            val table = new HTable(hbaseConf, tableName)
            val record = vertex.split(" ")
            val vertexId = record(0)
            val vertexUrl = record(1)
            // Row key is URL name
            val putter = new Put(Bytes.toBytes(vertexUrl))
            val dataFamilyName = Bytes.toBytes("Data")
            val vertexIdQualifierName = Bytes.toBytes("VertexId")
            val vertexIdValue = Bytes.toBytes(vertexId)
            putter.addColumn(dataFamilyName, vertexIdQualifierName, vertexIdValue)
            val urlQualifierName = Bytes.toBytes("URL")
            val urlValue = Bytes.toBytes(vertexUrl)
            putter.addColumn(dataFamilyName, urlQualifierName, urlValue)
            table.put(putter)
            table.close()
        }

        Console.print(vertexIds.map(putInHBase).count())
    }
}
