import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object edge_list {
    def main(args: Array[String]) {

        // setup the Spark Context
        val conf = new SparkConf().setAppName("CreateEdgeListFile")
        val sc = new SparkContext(conf)

        val file_name = "hdfs://ip-172-31-10-101:9000/common-crawl/crawl-data/CC-MAIN-2015-18/segments/1429246633512.41/warc/warc-edges-00000"

        // function to hash "(src_url, dst_url)" to integers
        def hash_record(record: String): String = {
            val error = "error".hashCode.toString
            val r = record.split(", ")
            // Catch ArrayIndexOutOfBoundsException
            try {
                val src_url = r(0).replace("(", "")
                val dst_url = r(1).replace(")", "")
                src_url.hashCode.toString + " " + dst_url.hashCode.toString
            } catch {
                case NonFatal(exc) => error + " " + error
            }
        }


        // read in the data from HDFS
        val rdd = sc.textFile(file_name)

        // map each record into a tuple consisting of the hash codes of (src_url, dst_url)
        val vertices = rdd.map(record => hash_record(record))

        // save the data back into HDFS
        val output_file_name = "hdfs://ip-172-31-10-101:9000/data/edge-lists/edge-list-file-0000"
        vertices.saveAsTextFile(output_file_name)
    }
}
