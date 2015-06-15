import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal

object pageRank {
    def main(args: Array[String]) {

        // setup the Spark Context
        val conf = new SparkConf().setAppName("CreateEdgeListFile")
        val sc = new SparkContext(conf)

        val warcFileEdges = "hdfs://ip-172-31-10-101:9000/common-crawl/crawl-data/CC-MAIN-2015-18/segments/1429246633512.41/warc/warc-edges-00000"
        val edgeListFiles = "hdfs://ip-172-31-10-101:9000/data/edge-lists"

        // function to map src_url to its hash integer
        def mapVertexHash(record: String): (Long, String) = {
            val error = "error".hashCode.toLong
            val r = record.split(", ")
            // Catch ArrayIndexOutOfBoundsException
            try {                val src_url = r(0).replace("(", "")
                (src_url.hashCode.toLong, src_url)
            } catch {
                case NonFatal(exc) => (error, "error")
            }
        }

        // read in the data from HDFS
        val rdd = sc.textFile(warcFileEdges)

        // map each VertexName to its VertexId
        val vertices = rdd.map(mapVertexHash).reduceByKey((a, b) => a)
        val verticesFileName = "hdfs://ip-172-31-10-101:9000/data/vertices/vertices-file-0000"

        // Setup GraphX graph
        val graph = GraphLoader.edgeListFile(sc, edgeListFiles)
        // Run PageRank
        val ranks = graph.pageRank(0.0001).vertices

        // Map VertexIds to URL
        val ranksByVertexId = vertices.join(ranks).map { case (id, (vid, rank)) => (vid, rank) }

        Console.print(ranksByVertexId.take(10).mkString("\n") + "\n")
    }
}
