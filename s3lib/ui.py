import argparse
from os.path import expanduser
from s3lib import Connection
from s3lib import sign
from safeoutput import open as safeopen

def load_creds(path):
  with open(path, "r") as f:
    access_id = f.readline().strip()
    secret_key = f.readline().strip()
    return (access_id, secret_key)

_BUFFSIZE = 65536
def copy(src, dst):
      buf = src.read(_BUFFSIZE)
      while len(buf) > 0:
        dst.write(buf)
        buf = src.read(_BUFFSIZE)

ls_parser = argparse.ArgumentParser("Program lists all the objects in an s3 bucket. Works on really big buckets")
ls_parser.add_argument('--host', type=str, dest='host', help='Name of host')
ls_parser.add_argument('--port', type=int, dest='port', help='Port to connect to')
ls_parser.add_argument('--output', type=str, dest='output', default=None, help='Name of output')
ls_parser.add_argument('--creds', type=str, dest='creds', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
ls_parser.add_argument('--mark', type=str, dest='mark', help='Starting point for enumeration')
ls_parser.add_argument('--prefix', type=str, dest='prefix', help='Prefix to match on')
ls_parser.add_argument('--batch', type=str, dest='batch', help='Batch size for s3 queries')
ls_parser.add_argument('bucket', type=str, nargs="?", default=None, help='Name of bucket')

def ls_main():
  args = ls_parser.parse_args()
  (access_id, secret_key) = load_creds(args.creds)
  with Connection(access_id, secret_key, args.host, args.port) as s3:
    with safeopen(args.output) as outfile:
      if args.bucket:
        keys = s3.list_bucket(args.bucket, args.mark, args.prefix, args.batch)
        for key in keys:
          print >> outfile, key
      else:
        buckets = s3.list_buckets()
        for bucket in buckets:
          print >> outfile, bucket

get_parser = argparse.ArgumentParser("Program lists all the objects in an s3 bucket. Works on really big buckets")
get_parser.add_argument('--host', type=str, dest='host', help='Name of host')
get_parser.add_argument('--port', type=int, dest='port', help='Port to connect to')
get_parser.add_argument('--output', type=str, dest='output', default=None, help='Name of output')
get_parser.add_argument('--creds', type=str, dest='creds', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
get_parser.add_argument('--mark', type=str, dest='mark', help='Starting point for enumeration')
get_parser.add_argument('--prefix', type=str, dest='prefix', help='Prefix to match on')
get_parser.add_argument('--batch', type=str, dest='batch', help='Batch size for s3 queries')
get_parser.add_argument('bucket', type=str, help='Name of bucket')
get_parser.add_argument('key', type=str, help='Name of key')

def get_main():
  args = get_parser.parse_args()
  (access_id, secret_key) = load_creds(args.creds)
  with Connection(access_id, secret_key, args.host, args.port) as s3:
    with safeopen(args.output) as outfile:
      data = s3.get_object(args.bucket, args.key)
      copy(data, outfile)

cp_parser = argparse.ArgumentParser("Program copies an object from one location to another")
cp_parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
cp_parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
cp_parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
cp_parser.add_argument('--header', type=str, dest='headers', default=[], action='store', nargs='*')
cp_parser.add_argument('src_bucket', type=str)
cp_parser.add_argument('src_object', type=str)
cp_parser.add_argument('dst_bucket', type=str)
cp_parser.add_argument('dst_object', type=str)

def cp_main():
  args = cp_parser.parse_args()
  (access_id, secret_key) = load_creds(args.creds)
  with Connection(access_id, secret_key, args.host, args.port) as s3:
    headers = {}
    for header in args.headers:
      try:
        (key, value) = header.split(':', 1)
        headers[key] = value
      except ValueError:
        raise ValueError("Header '%s' is not of form key:value" % header)
    s3.copy_object(args.src_bucket, args.src_object, args.dst_bucket, args.dst_object, headers)
    for (header, value) in headers:
      print "%s: %s" % (header, value, )

head_parser = argparse.ArgumentParser("Program lists all the objects in an s3 bucket. Works on really big buckets")
head_parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
head_parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
head_parser.add_argument('--json', action='store_true', help='Print in json format')
head_parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
head_parser.add_argument('bucket', type=str, action='store', help='Name of bucket')
head_parser.add_argument('objects', type=str, action='store', nargs='+', help='List of urls to query')

def head_main():
  args = head_parser.parse_args()
  (access_id, secret_key) = load_creds(args.creds)
  with Connection(access_id, secret_key, args.host, args.port) as s3:
    for obj in args.objects:
      headers = s3.head_object(args.bucket, obj)
      if args.json:
        print json.dumps({"object":obj, "headers":dict(headers)})
      else:
        for (header,value) in headers:
          print "%s: %s" % (header, value)

put_parser = argparse.ArgumentParser("Program puts an object into s3")
put_parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
put_parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
put_parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
put_parser.add_argument('--header', type=str, dest='headers', default=[], action='store', nargs='*')
put_parser.add_argument('bucket', type=str)
put_parser.add_argument('object', type=str)
put_parser.add_argument('file', type=str)

def put_main():
  args = put_parser.parse_args()
  (access_id, secret_key) = load_creds(args.creds)
  headers = {}
  for header in args.headers:
    try:
      (key, value) = header.split(':', 1)
      headers[key] = value
    except ValueError:
      raise ValueError("Header '%s' is not of form key:value" % header)
  with Connection(access_id, secret_key, args.host, args.port) as s3:
    with open(args.file, "r") as f:
      (status, headers) = s3.put_object(args.bucket, args.object, f, headers)
    print "HTTP Code: ", status
    for (header, value) in headers:
      print "%s: %s" % (header, value)
    print ""

rm_parser = argparse.ArgumentParser("Program deletes s3 keys.")
rm_parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
rm_parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
rm_parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
rm_parser.add_argument('bucket', type=str, action='store', help='Name of bucket')
rm_parser.add_argument('objects', type=str, action='store', nargs='+', help='List of urls to query')

def rm_main():
  args = rm_parser.parse_args()
  (access_id, secret_key) = load_creds(args.creds)
  with Connection(access_id, secret_key, args.host, args.port) as s3:
    for obj in args.objects:
      status, headers = s3.delete_object(args.bucket, obj)

sign_parser = argparse.ArgumentParser("Sign an S3 form.")
sign_parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')
sign_parser.add_argument('file', type=str)

def sign_main():
  args = sign_parser.parse_args()
  (_, secret_key) = load_creds(args.creds)
  with open(args.file, 'r') as f:
    policy_document = f.read()
  policy = base64.b64encode(policy_document)
  signature = sign(policy)
  print policy
  print signature
