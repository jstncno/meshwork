import re, boto, gzip, warc, os
from urlparse import urlparse
from collections import Counter
from boto.s3.key import Key
from gzipstream import GzipStreamFile

def get_tag_count(data, ctr=None):
    if ctr is None:
        ctr = Counter()
    ctr.update(HTML_TAG_PATTERN.findall(data.lower()))
    return ctr

HTML_TAG_PATTERN = re.compile('<([a-z0-9]+)[^>]*>')
assert get_tag_count('<html><a href="..."></a><h1 /><br/><p><p></p></p>') == {'html': 1, 'a': 1, 'p': 2, 'h1': 1, 'br': 1}

def get_links(data):
    links = re.findall(r'href=[\'"]?([^\'" >]+)', data)
    return [l for l in set(map(lambda link: urlparse(link).netloc, links)) if l]

def process_record(record):
    if record['Content-Type'] == 'application/http; msgtype=response':
        payload = record.payload.read()
        headers, body = payload.split('\r\n\r\n', 1)
        if 'Content-Type: text/html' in headers:
            url = urlparse(record['WARC-Target-URI']).netloc
            links = get_links(body)
            return url, links
    return None, None

BUCKET_NAME = 'aws-publicdatasets'
KEY = None
with open('warc.path', 'r') as f:
    KEY = f.read().strip()

conn = boto.connect_s3(anon=True)
bucket = conn.get_bucket(BUCKET_NAME)
k = Key(bucket, KEY)
f = warc.WARCFile(fileobj=GzipStreamFile(k))

for record in f:
    if record['Content-Type'] == 'application/http; msgtype=response':
        url, links = process_record(record)
        if links:
            for link in links:
                print url, link
            break
    #with open('temp.txt', 'a') as tempfile:


import sys
sys.exit()

for record in f:
    for key, value in process_record(record):
        print key, value
