#!/usr/bin/env python

import argparse
from os.path import expanduser
from s3_lib import put_object

parser = argparse.ArgumentParser("Program puts an object into s3")

parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
parser.add_argument('--header', type=str, dest='headers', default=[], action='store', nargs='*')
parser.add_argument('bucket', type=str)
parser.add_argument('object', type=str)
parser.add_argument('file', type=str)

def main():
    args = parser.parse_args()
    (access_id, secret_key) = load_creds(args.creds)
    headers = {}
    for header in args.headers:
        try:
            (key, value) = header.split(':', 1)
            headers[key] = value
        except ValueError:
            raise ValueError("Header '%s' is not of form key:value" % header)
    with open(args.file, "r") as f:
        data = f.read()
    (status, headers, xml) = put_object(args.bucket, args.object, data, args.host, args.port, headers, access_id, secret_key)
    print "HTTP Code: ", status
    for (header, value) in headers:
        print "%s: %s" % (header, value, )
    print ""
    print xml

def load_creds(path):
    with open(path, "r") as f:
        access_id = f.readline().strip()
        secret_key = f.readline().strip()
        return (access_id, secret_key)

if __name__ == '__main__':
    main()

