import re, boto, gzip, warc, os, time, sys

from pyspark import SparkContext, SparkConf

def main():
    link_edges_file_path = 'hdfs://{}:9000/data/link-edges'.format(os.environ['MASTER_NAME'])
    normalized_link_edges_path = 'hdfs://{}:9000/data/normalized-link-edges'.format(os.environ['MASTER_NAME'])
    conf = SparkConf().setAppName('NormalizeLinks')
    sc = SparkContext(conf=conf)
    link_edges = sc.textFile(link_edges_file_path).map(lambda line: line.lower().replace('www.', ''))
    print link_edges.count()
    print link_edges.take(10)
    # Delete existing /data/link-edges directory...
    os.system('hdfs dfs -rm -r -f /data/normalized-link-edges')
    link_edges.saveAsTextFile(normalized_link_edges_path)

if __name__ == '__main__':
    main()
