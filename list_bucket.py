#!/usr/bin/env python

import httplib
import argparse
import time

parser = argparse.ArgumentParser("Program lists all the objects in an s3 bucket. Works on really big buckets")

parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
parser.add_argument('--bucket', type=str, dest='bucket', action='store', required=True, help='Name of bucket')
parser.add_argument('--output', type=str, dest='output', action='store', default='/dev/stdout', help='Name of output')

def main():
    args = parser.parse_args()
    fetch_bucket_part(args.bucket, args.host, args.port)

def fetch_bucket_part(bucket, host, port, start=None, max_items=None):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S GMT', time.gmtime())

    args = []
    if start:
        args.append( "marker=%s" % (start) )
    if max_items:
        args.append( "max-keys=%s" % (max_items) )
    path = "/?" + "&".join(args)

    headers = {}
    headers["Host"]          = "%s.%s" % (bucket, host)
    headers["Date"]          = http_now
    # headers["Authorization"] = signatureValue
    headers["Content-Type"]  = "text/plain"

    conn = httplib.HTTPConnection(host, port)
    conn.request("GET", path, "", headers)
    resp = conn.getresponse()
    print resp.status
    print  resp.reason
    print  resp.getheaders()
    print  resp.read()

if __name__ == '__main__':
    main()

