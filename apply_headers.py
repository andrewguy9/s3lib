#!/usr/bin/env python

import argparse
from os.path import expanduser
from s3_lib import copy_object

parser = argparse.ArgumentParser("Program applies headers for a list of objects.")

parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
parser.add_argument('--header', type=str, dest='headers', default=[], action='store', nargs='*')
parser.add_argument('bucket', type=str)
parser.add_argument('files', type=str, default=['/dev/stdin'], action='store', nargs='*')

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
    for path in args.files:
        with open(path, "r") as f:
            for obj in f.readlines():
                obj = obj.strip()
                try:
                    (status, resp_headers, xml) = copy_object(args.bucket, obj, args.bucket, obj, args.host, args.port, headers, access_id, secret_key)
                except ValueError as e:
                    print 'ERROR', obj, e
                else:
                    print 'OK   ', obj

def load_creds(path):
    with open(path, "r") as f:
        access_id = f.readline().strip()
        secret_key = f.readline().strip()
        return (access_id, secret_key)

if __name__ == '__main__':
    main()

