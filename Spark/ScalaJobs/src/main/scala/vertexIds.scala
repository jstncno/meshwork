import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal
import java.security.MessageDigest
import java.nio.ByteBuffer
import scala.sys.process._

object vertexIds {
    def main(args: Array[String]) {

        // setup the Spark Context
        val conf = new SparkConf().setAppName("CreateVertexIds")
        val sc = new SparkContext(conf)

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
        val edgeList = rdd.map(vertexIdHash).flatMap(record => record).distinct()

        // delete existing directory
        "hdfs dfs -rm -r -f /data/vertex-ids" !
        // save the data back into HDFS
        val vertexIdFileName = hdfsPath+"/data/vertex-ids"
        edgeList.saveAsTextFile(vertexIdFileName)

        Console.print("Vertex ID file saved to /data/vertex-ids\n")

    }
}
