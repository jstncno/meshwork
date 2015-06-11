import re, boto, gzip, warc, os
from urlparse import urlparse
from collections import Counter
from boto.s3.key import Key
from gzipstream import GzipStreamFile

### GLOBALS ###
BUCKET_NAME = 'aws-publicdatasets'
KEY = None
PATH = None
DIR = None
COUNTER = 0

def get_tag_count(data, ctr=None):
    if ctr is None:
        ctr = Counter()
    ctr.update(HTML_TAG_PATTERN.findall(data.lower()))
    return ctr

HTML_TAG_PATTERN = re.compile('<([a-z0-9]+)[^>]*>')
assert get_tag_count('<html><a href="..."></a><h1 /><br/><p><p></p></p>') == {'html': 1, 'a': 1, 'p': 2, 'h1': 1, 'br': 1}

def get_links(data):
    links = re.findall(r'href=[\'"]?([^\'" >]+)', data)
    try:
        links = set(map(lambda link: urlparse(link).netloc, links))
        return [l.replace('www.', '') for l in links if l]
    except ValueError:
        return None

def process_record(record):
    if record['Content-Type'] == 'application/http; msgtype=response':
        payload = record.payload.read()
        headers, body = payload.split('\r\n\r\n', 1)
        if 'Content-Type: text/html' in headers:
            url = urlparse(record['WARC-Target-URI']).netloc
            links = get_links(body)
            if links:
                return url, links
    return None, None

def write_to_local_file(data):
    global COUNTER
    ext = '-{}'.format(str(COUNTER).zfill(5))
    filename = PATH[1].replace('.warc.gz', ext)
    tempfile = open(filename, 'a+')

    statinfo = os.stat(filename)
    tempfile_size = statinfo.st_size/float(1024*1024) # size in MB
    if tempfile_size >= 128:
        print "Loading next file!"
        COUNTER += 1
        tempfile.close()
        copy_to_HDFS(filename)
        write_to_local_file(data)
        return
    else:
        tempfile.write(data)

    tempfile.close()

def copy_to_HDFS(filename):
    print 'Copying {} to HDFS!'.format(filename)
    hdfs_filename = os.path.join(DIR, filename)
    os.system('hdfs dfs -mkdir -p {}'.format(DIR))
    os.system('hdfs dfs -copyFromLocal {} {}'.format(filename, hdfs_filename))
    os.system('rm {}'.format(filename))

conn = boto.connect_s3(anon=True)
bucket = conn.get_bucket(BUCKET_NAME)
with open('warc-10.paths', 'r') as f:
    global BUCKET_NAME, KEY, PATH, DIR
    for line in f:
        KEY = line.strip()

    PATH = os.path.split(KEY)
    DIR = PATH[0]

    k = Key(bucket, KEY)
    f = warc.WARCFile(fileobj=GzipStreamFile(k))

    for record in f:
        if record['Content-Type'] == 'application/http; msgtype=response':
            url, links = process_record(record)
            if links:
                for link in links:
                    write_to_local_file('({}, {})\n'.format(url, link))


