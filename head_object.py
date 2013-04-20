#!/usr/bin/env python

import argparse
from os.path import expanduser
from s3_lib import head_object

parser = argparse.ArgumentParser("Program lists all the objects in an s3 bucket. Works on really big buckets")

parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
parser.add_argument('--bucket', type=str, dest='bucket', action='store', required=True, help='Name of bucket')
parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
parser.add_argument('objects', type=str, action='store', nargs='+', help='List of urls to query')

def main():
    args = parser.parse_args()
    (access_id, secret_key) = load_creds(args.creds)
    for obj in args.objects:
        headers = head_object(args.bucket, obj, args.host, args.port, access_id, secret_key)
        for (header,value) in headers:
            print "%s: %s" % (header, value)

def load_creds(path):
    with open(path, "r") as f:
        access_id = f.readline().strip()
        secret_key = f.readline().strip()
        return (access_id, secret_key)

if __name__ == '__main__':
    main()

