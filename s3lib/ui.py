from __future__ import print_function
import argparse
import base64
from os.path import expanduser
from s3lib import Connection
from s3lib import sign
from safeoutput import open as safeopen
from os import environ
from docopt import docopt
import json

def load_creds_from_file(path):
  with open(path, "r") as f:
    access_id = f.readline().strip()
    secret_key = f.readline().strip().encode('ascii')
    return (access_id, secret_key)

def load_creds_from_vars():
  access_id = environ.get('AWS_ACCESS_KEY_ID')
  secret_key = environ.get('AWS_SECRET_ACCESS_KEY')
  if  access_id is not None and secret_key is not None:
    return (access_id, secret_key.encode('ascii'))
  else:
    return None

def load_creds(path):
  """
  path is str
  returns (access_id, secret) with types (?, bytes)
  """
  # Use the path if provided.
  if path is not None:
    return load_creds_from_file(path)
  # Use env vars if provided
  creds = load_creds_from_vars()
  if creds is not None:
    return creds
  # Use home dir if provided
  return load_creds_from_file(expanduser("~/.s3"))

_BUFFSIZE = 65536
def copy(src, dst):
      buf = src.read(_BUFFSIZE)
      while len(buf) > 0:
        dst.write(buf)
        buf = src.read(_BUFFSIZE)

LS_USAGE = """
s3ls -- Program lists all the objects in an s3 bucket. Works on really big buckets

Usage:
    s3ls [options] [<bucket>]

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --output=<output>   Name of output.
    --creds=<creds>     Name of file to find aws access id and secret key.
    --mark=<mark>       Starting point for enumeration.
    --prefix=<prefix>   Prefix to match on.
    --batch=<batch>     Batch size for s3 queries [default: 1000].
"""

def ls_main(argv=None):
  args = docopt(LS_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port')) as s3:
    with safeopen(args.get('--output')) as outfile:
      bucket = args.get('<bucket>')
      if bucket:
        keys = s3.list_bucket(bucket, start=args.get('--mark'), prefix=args.get('--prefix'), batch_size=args.get('--batch'))
        for key in keys:
          print(key, file=outfile)
      else:
        buckets = s3.list_buckets()
        for bucket in buckets:
          print(bucket, file=outfile)

GET_USAGE = """
s3get -- Program reads an object in an s3 bucket.

Usage:
    s3ls [options] <bucket> <key>

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --output=<output>   Name of output.
    --creds=<creds>     Name of file to find aws access id and secret key.
"""

def get_main(argv=None):
  args = docopt(GET_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port')) as s3:
    with safeopen(args.get('--output'), 'wb') as outfile:
      data = s3.get_object(args.get('<bucket>'), args.get('<key>'))
      copy(data, outfile)

CP_USAGE="""
s3cp -- Program copies an object from one location to another.

Usage:
    s3cp [options] <src_bucket> <src_object> <dst_bucket> <dst_object> [--header=<header>]...

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --creds=<creds>     Name of file to find aws access id and secret key.
"""

def cp_main(argv=None):
  args = docopt(CP_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port')) as s3:
    headers = {}
    for header in args.get('--header'):
      try:
        (key, value) = header.split(':', 1)
        headers[key] = value
      except ValueError:
        raise ValueError("Header '%s' is not of form key:value" % header)
    (status, headers) = s3.copy_object(args.get('<src_bucket>'), args.get('<src_object>'), args.get('<dst_bucket>'), args.get('<dst_object>'), headers)
    print("HTTP Code: ", status)
    for (header, value) in headers:
      print("%s: %s" % (header, value, ))

HEAD_USAGE = """
s3head -- Program gets metadata on s3 object.

Usage:
    s3head [options] <bucket> <object>...

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --creds=<creds>     Name of file to find aws access id and secret key.
    --json              Print in json format.
"""

def head_main(argv=None):
  args = docopt(HEAD_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port')) as s3:
    for obj in args.get('<object>'):
      headers = s3.head_object(args.get('<bucket>'), obj)
      if args.get('--json'):
        print(json.dumps({"object":obj, "headers":dict(headers)}))
      else:
        for (header,value) in headers:
          print("%s: %s" % (header, value))

PUT_USAGE = """
s3put -- Program puts an object into s3.

Usage:
    s3put [options] [--header=<header>]... <bucket> <object> <file>

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --creds=<creds>     Name of file to find aws access id and secret key.
"""

def put_main(argv=None):
  args = docopt(PUT_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  headers = {}
  for header in args.get('--header'):
    try:
      (key, value) = header.split(':', 1)
      headers[key] = value
    except ValueError:
      raise ValueError("Header '%s' is not of form key:value" % header)
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port')) as s3:
    with open(args.get('<file>'), 'rb') as f:
      (status, headers) = s3.put_object(args.get('<bucket>'), args.get('<object>'), f.read(), headers)
    print("HTTP Code: ", status)
    for (header, value) in headers:
      print("%s: %s" % (header, value))

RM_USAGE = """
s3rm -- Program deletes s3 keys.

Usage:
    s3rm [options] <bucket> <object>...

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --creds=<creds>     Name of file to find aws access id and secret key.
    -v, --verbose       Be verbose when deleting files, showing them as they are removed.
    --batch=<batch>     Batch size for s3 queries [default: 500].
"""

def rm_main(argv=None):
  args = docopt(RM_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port')) as s3:
    for (key, result) in s3.delete_objects(args.get('<bucket>'), args.get('<object>'), int(args.get('--batch')), not args.get('--verbose')):
      print(key, result)

SIGN_USAGE = """
s3sign -- Sign an S3 form.

Usage:
    s3sign [options] <file>

Options:
    --creds=<creds>     Name of file to find aws access id and secret key.
"""

def sign_main(argv=None):
  args = docopt(SIGN_USAGE, argv)
  (_, secret_key) = load_creds(args.get('--creds'))
  with open(args.get('<file>'), 'rb') as f:
    policy_document = f.read()
  policy = base64.b64encode(policy_document)
  signature = sign(secret_key, policy)
  print(policy.decode())
  print(signature.decode())
