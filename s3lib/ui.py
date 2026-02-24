from binascii import b2a_base64
from http.client import HTTPResponse
from pathlib import Path
from . import Connection, LIST_BUCKET_ATTRIBUTES, LIST_BUCKET_CHECKSUM_ATTRIBUTES, LIST_BUCKET_KEY, sign
from base64 import b64encode
from docopt import docopt
from json import dumps
from os import PathLike, environ
from os.path import expanduser
from safeoutput import open as safeopen
import sys
from typing import BinaryIO, Tuple, cast

def load_creds_from_file(path: PathLike) -> Tuple[str, bytes]:
  with open(path, "r") as f:
    access_id = f.readline().strip()
    secret_key = f.readline().strip().encode('ascii')
    return (access_id, secret_key)

def load_creds_from_vars() -> Tuple[str, bytes] | None:
  access_id = environ.get('AWS_ACCESS_KEY_ID')
  secret_key = environ.get('AWS_SECRET_ACCESS_KEY')
  if  access_id is not None and secret_key is not None:
    return (access_id, secret_key.encode('ascii'))
  else:
    return None

def load_creds(path: PathLike | None) -> Tuple[str, bytes]:
  """
  returns (access_id, secret) with types (str, bytes)
  """
  # Use the path if provided.
  if path is not None:
    return load_creds_from_file(path)
  # Use env vars if provided
  creds = load_creds_from_vars()
  if creds is not None:
    return creds
  # Use home dir if provided
  home_cred_path = Path(expanduser("~/.s3"))
  return load_creds_from_file(home_cred_path)

_BUFFSIZE = 65536
def copy(src: BinaryIO, dst: BinaryIO):
      buf = src.read(_BUFFSIZE)
      while len(buf) > 0:
        dst.write(buf)
        buf = src.read(_BUFFSIZE)

LS_USAGE = """
s3ls -- Program lists all the objects in an s3 bucket. Works on really big buckets

Usage:
    s3ls [options] [<bucket>] [--fields <field>...]

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --output=<output>   Name of output.
    --creds=<creds>     Name of file to find aws access id and secret key.
    --mark=<mark>       Starting point for enumeration.
    --prefix=<prefix>   Prefix to match on.
    --batch=<batch>     Batch size for s3 queries [default: 1000].
    --http              Use HTTP instead of HTTPS (useful in VPCs).

Available fields:
    Standard: %s
    Checksums: %s

    Note: List only returns ChecksumAlgorithm and ChecksumType.
          Use s3head to get actual checksum values (SHA256, CRC64NVME, etc).
""" % (",".join(LIST_BUCKET_ATTRIBUTES), ",".join(LIST_BUCKET_CHECKSUM_ATTRIBUTES))

def ls_main(argv=None) -> None:
  args = docopt(LS_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  use_ssl = not args.get('--http')
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port'), use_ssl=use_ssl) as s3:
    with safeopen(args.get('--output')) as outfile:
      bucket = args.get('<bucket>')
      if bucket:
        objs = s3.list_bucket2(bucket, start=args.get('--mark'), prefix=args.get('--prefix'), batch_size=args.get('--batch'))
        fields = args.get('<field>') or [LIST_BUCKET_KEY]
        for obj in objs:
          # Use empty string for missing fields (e.g., checksums not present)
          selected = [obj.get(field) or '' for field in fields]
          print("\t".join(selected), file=outfile)
      else:
        buckets = s3.list_buckets()
        for bucket in buckets:
          print(bucket, file=outfile)

GET_USAGE = """
s3get -- Program reads an object in an s3 bucket.

Usage:
    s3get [options] <bucket> <key> [<file>]

Options:
    --host=<host>           Name of host.
    --port=<port>           Port to connect to.
    --creds=<creds>         Name of file to find aws access id and secret key.
    --no-verify-checksum    Disable checksum verification
    --if-match=<etag>       Only download if ETag matches (error if changed)
    --if-none-match=<etag>  Skip download if ETag matches (for caching)
    --http                  Use HTTP instead of HTTPS (useful in VPCs).
"""

def verified_copy(src: HTTPResponse, dst: BinaryIO, verify: bool = True) -> None:
  """
  Copy from HTTP response to destination, optionally verifying checksum.

  Args:
      src: HTTPResponse object (has .getheaders() and .read())
      dst: File-like object to write to
      verify: If True, verify x-amz-checksum-sha256/sha1/md5 if present

  Raises:
      ValueError: If checksum verification fails
  """
  # Check for available checksums (prefer SHA256 > SHA1 > MD5)
  headers = dict(src.getheaders())
  expected_checksum = None
  algorithm = None

  if headers.get('x-amz-checksum-sha256'):
    expected_checksum = headers.get('x-amz-checksum-sha256')
    algorithm = 'SHA256'
  elif headers.get('x-amz-checksum-sha1'):
    expected_checksum = headers.get('x-amz-checksum-sha1')
    algorithm = 'SHA1'
  elif headers.get('x-amz-checksum-md5'):
    expected_checksum = headers.get('x-amz-checksum-md5')
    algorithm = 'MD5'

  if verify and expected_checksum and algorithm:
    # Copy while hashing
    if algorithm == 'SHA256':
      from hashlib import sha256
      hasher = sha256()
    elif algorithm == 'SHA1':
      from hashlib import sha1
      hasher = sha1()
    elif algorithm == 'MD5':
      from hashlib import md5
      hasher = md5()

    buf = src.read(_BUFFSIZE)
    while len(buf) > 0:
      hasher.update(buf)
      dst.write(buf)
      buf = src.read(_BUFFSIZE)

    # Verify
    actual_checksum = b2a_base64(hasher.digest()).strip().decode('ascii')
    if actual_checksum != expected_checksum:
      raise ValueError(
        f"Checksum verification failed!\n"
        f"Algorithm: {algorithm}\n"
        f"Expected:  {expected_checksum}\n"
        f"Actual:    {actual_checksum}"
      )
  else:
    # No checksum or verification disabled - just copy
    copy(src, dst)

def get_main(argv=None) -> None:
  args = docopt(GET_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  bucket = args.get('<bucket>')
  key = args.get('<key>')
  file_path = args.get('<file>')
  verify = not args.get('--no-verify-checksum')
  use_ssl = not args.get('--http')

  with Connection(access_id, secret_key, args.get('--host'), args.get('--port'), use_ssl=use_ssl) as s3:
    # Use structured API for conditional requests
    # User provides ETags from command line (may or may not have quotes)
    # Strip quotes if present to ensure consistent format
    if_match = args.get('--if-match')
    if if_match:
      if_match = if_match.strip('"')

    if_none_match = args.get('--if-none-match')
    if if_none_match:
      if_none_match = if_none_match.strip('"')

    response = s3.get_object(bucket, key, if_match=if_match, if_none_match=if_none_match)

    # Handle conditional response codes
    if response.status == 304:
      # Not Modified - object hasn't changed, nothing to download
      print("304 Not Modified - object unchanged", file=sys.stderr)
      return
    elif response.status == 412:
      # Precondition Failed - ETag didn't match
      print("412 Precondition Failed - ETag mismatch", file=sys.stderr)
      sys.exit(1)
    elif response.status != 200:
      # Other error
      print(f"HTTP {response.status}", file=sys.stderr)
      sys.exit(1)

    # Download and verify
    if file_path is None:
      # Write to stdout - don't close it
      outfile = cast(BinaryIO, sys.stdout.buffer if hasattr(sys.stdout, 'buffer') else sys.stdout)
      verified_copy(response, outfile, verify=verify)
    else:
      # Write to file
      with open(file_path, 'wb') as outfile:
        verified_copy(response, outfile, verify=verify)

CP_USAGE="""
s3cp -- Program copies an object from one location to another.

Usage:
    s3cp [options] <src_bucket> <src_object> <dst_bucket> <dst_object> [--header=<header>]...

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --creds=<creds>     Name of file to find aws access id and secret key.
    --http              Use HTTP instead of HTTPS (useful in VPCs).
"""

def cp_main(argv=None) -> None:
  args = docopt(CP_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  use_ssl = not args.get('--http')
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port'), use_ssl=use_ssl) as s3:
    headers = {}
    for header in args.get('--header', []):
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
    --http              Use HTTP instead of HTTPS (useful in VPCs).
"""

def head_main(argv=None) -> None:
  args = docopt(HEAD_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  use_ssl = not args.get('--http')
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port'), use_ssl=use_ssl) as s3:
    for obj in args.get('<object>', []):
      headers = s3.head_object(args.get('<bucket>'), obj)
      if args.get('--json'):
        print(dumps({"object":obj, "headers":dict(headers)}))
      else:
        for (header,value) in headers:
          print("%s: %s" % (header, value))

PUT_USAGE = """
s3put -- Program puts an object into s3.

Usage:
    s3put [options] [--header=<header>]... <bucket> <object> [<file>]

Options:
    --host=<host>       Name of host.
    --port=<port>       Port to connect to.
    --creds=<creds>     Name of file to find aws access id and secret key.
    --no-checksum       Disable checksum calculation (for stdin uploads)
    --create-only       Only upload if object doesn't exist (returns 412 if exists)
    --if-match=<etag>   Only upload if current ETag matches (optimistic locking)
    --http              Use HTTP instead of HTTPS (useful in VPCs).
"""

def get_input_fd(path: PathLike | None) -> BinaryIO:
    if path is None:
        return cast(BinaryIO, sys.stdin.buffer if hasattr(sys.stdin, 'buffer') else sys.stdin)
    else:
        return open(path, 'rb')

def put_main(argv=None) -> None:
  args = docopt(PUT_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  use_ssl = not args.get('--http')
  headers = {}
  for header in args.get('--header', []):
    try:
      (key, value) = header.split(':', 1)
      headers[key] = value
    except ValueError:
      raise ValueError("Header '%s' is not of form key:value" % header)

  with Connection(access_id, secret_key, args.get('--host'), args.get('--port'), use_ssl=use_ssl) as s3:
    file_path = args.get('<file>')

    # Determine checksum setting
    if args.get('--no-checksum'):
      # User explicitly disabled checksumming
      checksum_algorithm = None
    else:
      # Let put_object use default behavior:
      # - stdin/bytes will get SHA256 auto-calculated
      # - files will remain as file objects and skip checksumming
      checksum_algorithm = None

    # Handle ETag for conditional upload
    # User provides ETag from command line (may or may not have quotes)
    # Strip quotes if present to ensure consistent format
    if_match = args.get('--if-match')
    if if_match:
      if_match = if_match.strip('"')

    # Read stdin into bytes so checksums can be calculated
    # For regular files, pass file object to avoid memory issues
    if file_path is None:
      # stdin - read into bytes for checksum calculation
      if hasattr(sys.stdin, 'buffer'):
        data = sys.stdin.buffer.read()
      else:
        data = sys.stdin.read().encode('utf-8')
    else:
      # Regular file - pass as file object
      data = open(file_path, 'rb')

    try:
      (status, resp_headers) = s3.put_object(
        args.get('<bucket>'),
        args.get('<object>'),
        data,
        headers,
        checksum_algorithm=checksum_algorithm,
        if_none_match=args.get('--create-only'),
        if_match=if_match
      )
    finally:
      # Close file if we opened it
      if file_path is not None and hasattr(data, 'close'):
        data.close()
    print("HTTP Code: ", status)
    for (header, value) in resp_headers:
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
    --http              Use HTTP instead of HTTPS (useful in VPCs).
"""

def rm_main(argv=None) -> None:
  args = docopt(RM_USAGE, argv)
  (access_id, secret_key) = load_creds(args.get('--creds'))
  use_ssl = not args.get('--http')
  with Connection(access_id, secret_key, args.get('--host'), args.get('--port'), use_ssl=use_ssl) as s3:
    batch_size_str = args.get('--batch')
    assert batch_size_str is not None # docopt provides default string.
    for (key, result) in s3.delete_objects(args.get('<bucket>'), args.get('<object>'), int(batch_size_str), not args.get('--verbose')):
      print(key, result)

SIGN_USAGE = """
s3sign -- Sign an S3 form.

Usage:
    s3sign [options] <file>

Options:
    --creds=<creds>     Name of file to find aws access id and secret key.
"""

def sign_main(argv=None) -> None:
  args = docopt(SIGN_USAGE, argv)
  (_, secret_key) = load_creds(args.get('--creds'))
  file_path = args.get('<file>')
  assert file_path is not None  # docopt ensures required arg
  with open(file_path, 'rb') as f:
    policy_document = f.read()
  policy = b64encode(policy_document)
  signature = sign(secret_key, policy)
  print(policy.decode())
  print(signature.decode())
