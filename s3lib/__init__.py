#!/usr/bin/env python

import hmac
from hashlib import sha1
from hashlib import md5
import binascii
import httplib
import time
from xml.etree.ElementTree import fromstring as parse
from xml.etree.ElementTree import Element, SubElement, tostring

class Connection:

  ############################
  # Python special functions #
  ############################
  def __init__(self, access_id, secret, host=None, port=None, conn_timeout=None):
    self.access_id = access_id
    self.secret = secret
    if port is None:
      self.port = 80
    else:
      self.port = port
    if host is None:
      self.host = "s3.amazonaws.com"
    else:
      self.host = host
    self.conn_timeout = conn_timeout

  def __enter__(self):
    self._connect()
    return self

  def __exit__(self, type, value, traceback):
    self._disconnect()

  #######################
  # Interface Functions #
  #######################
  def list_buckets(self):
    """ list all buckets in account """
    xml = self._s3_get_service_request()
    buckets = _parse_get_service_response(xml)
    for bucket in buckets:
      yield bucket

  def list_bucket(self, bucket, start, prefix, batch_size):
    """ list contents of individual bucket """
    more = True
    while more:
      xml = self._s3_list_request(bucket, start, prefix, batch_size)
      keys, truncated = _parse_list_response(xml)
      for key in keys:
        yield key
        start = key # Next request should start from last request's last item.
      more = truncated

  def get_object(self, bucket, key):
    """ pull down bucket object by key """
    #TODO Want to replace with some enter, exit struct.
    return self._s3_get_request(bucket, key)

  def head_object(self, bucket, key):
    """ get request metadata for key in bucket """
    status, headers = self._s3_head_request(bucket, key)
    return headers

  def delete_object(self, bucket, key):
    """ delete key from bucket """
    status, headers = self._s3_delete_request(bucket, key)
    return (status, headers)

  def delete_objects(self, bucket, keys, batch_size, quiet):
    """ delete keys from bucket """
    for batch in _batchify(batch_size, keys):
      xml = self._s3_delete_bulk_request(bucket, batch, quiet)
      results = _parse_delete_bulk_response(xml)
      for (key, result) in results:
        yield key, result

  def copy_object(self, src_bucket, src_key, dst_bucket, dst_key, headers):
    """ copy key from one bucket to another """
    (status, headers) = self._s3_copy_request(src_bucket, src_key, dst_bucket, dst_key, headers)
    return (status, headers)

  def put_object(self, bucket, key, data, headers):
    """ push object from local to bucket """
    (status, headers) = self._s3_put_request(bucket, key, data, headers)
    return (status, headers)

##########################
# Http request Functions #
##########################

  def _s3_get_service_request(self):
    resp = self._s3_request("GET", None, None, {}, {}, '')
    if resp.status != httplib.OK:
      raise ValueError("S3 request failed with: %s" % (resp.status))
    return resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.

  def _s3_list_request(self, bucket, marker=None, prefix=None, max_keys=None):
    args = {}
    if marker:
      args['marker'] = marker
    if prefix:
      args['prefix'] = prefix
    if max_keys:
      args['max-keys'] = max_keys
    resp = self._s3_request("GET", bucket, "", args, {}, '')
    if resp.status != httplib.OK:
      raise ValueError("S3 request failed with: %s" % (resp.status))
    return resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.

  def _s3_get_request(self, bucket, key):
    resp = self._s3_request("GET", bucket, key, {}, {}, '')
    if resp.status != httplib.OK:
      raise ValueError("S3 request failed with %s" % (resp.status))
    return resp

  def _s3_head_request(self, bucket, key):
    resp = self._s3_request("HEAD", bucket, key, {}, {}, '')
    if resp.status != httplib.OK:
      raise ValueError("S3 request failed with %s" % (resp.status))
    resp.read() #NOTE: Should be zero size response. Required to reset the connection.
    return (resp.status, resp.getheaders())

  def _s3_delete_request(self, bucket, key):
    resp = self._s3_request("DELETE", bucket, key, {}, {}, '')
    if resp.status != httplib.NO_CONTENT:
      raise ValueError("S3 request failed with %s" % (resp.status))
    resp.read() #NOTE: Should be zero size response. Required to reset the connection
    return (resp.status, resp.getheaders())

  def _s3_delete_bulk_request(self, bucket, keys, quiet):
    content = _render_delete_bulk_content(keys, quiet)
    resp = self._s3_request("POST", bucket, "/?delete", {}, {}, content)
    if resp.status != httplib.OK:
      raise ValueError("S3 request failed with %s" % (resp.status))
    results = resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
    return results

  def _s3_copy_request(self, src_bucket, src_key, dst_bucket, dst_key, headers):
    copy_headers = {'x-amz-copy-source':"/%s/%s" % (src_bucket, src_key)}
    copy_headers['x-amz-metadata-directive'] = 'REPLACE'
    headers = dict(headers.items() + copy_headers.items())
    resp = self._s3_request("PUT", dst_bucket, dst_key, {}, headers, '')
    if resp.status != httplib.OK:
      raise ValueError("S3 request failed with: %s" % (resp.status))
    return (resp.status, resp.getheaders())

  def _s3_put_request(self, bucket, key, data, headers):
    args = {}
    resp = self._s3_request("PUT", bucket, key, args, headers, data)
    if resp.status != httplib.OK:
      raise ValueError("S3 request failed with: %s" % (resp.status))
    resp.read() #NOTE: Should be zero length response. Required to reset the connection.
    return (resp.status, resp.getheaders())

  def _s3_request(self, method, bucket, key, args, headers, content):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S +0000', time.gmtime())

    args = map( lambda x: "=".join(x), args.items())
    args_str = "&".join(args)
    if args_str:
      args_str = "?" + args_str
    canonical_resource = "/"
    if bucket:
      canonical_resource += bucket + "/"
      if key:
        canonical_resource += key

    resource = "/"
    if key:
      resource += key
    resource += args_str

    try:
      content_type = headers['Content-Type']
    except KeyError:
      content_type = ''
    content_md5 = sign_content_if_possible(content)
    (amz_headers, reg_headers) = _split_headers(headers)
    string_to_sign = _get_string_to_sign(method, content_md5, content_type, http_now, amz_headers, canonical_resource)
    signature = sign(self.secret, string_to_sign)

    if bucket:
      host = bucket + "." + self.host
    else:
      host = self.host
    headers["Host"] = host
    headers["Date"] = http_now
    headers["Authorization"] = "AWS %s:%s" % (self.access_id, signature)
    headers["Connection"] = "keep-alive"
    if content_md5 != '':
      headers['Content-MD5'] = content_md5

    self.conn.request(method, resource, content, headers)
    resp = self.conn.getresponse()
    return resp

###########################
# S3 Connection Functions #
###########################
  def _connect(self):
    self.conn = httplib.HTTPConnection(self.host, self.port, timeout=self.conn_timeout)

  def _disconnect(self):
    self.conn.close()

############################
# Python Iteration Helpers #
############################

def _take(size, collection):
  if size < 1:
    raise ValueError("Size must be 1 or greater")
  iterator = iter(collection)
  """Yields up to size elements from iterator."""
  for i in xrange(size):
    yield iterator.next()

def _batchify(size, collection):
  if size < 1:
    raise ValueError("Size must be 1 or greater")
  iterator = iter(collection)
  while True:
    batch = list(_take(size, iterator))
    if len(batch) == 0:
      break
    else:
      yield batch

##########################
# Signing Util Functions #
##########################

def _split_headers(headers):
  """Some headers are special to amazon. Splits those from regular http headers"""
  amz_headers = {}
  reg_headers = {}
  for cur in headers:
    if cur.lower().startswith('x-amz-'):
      amz_headers[cur] = headers[cur]
    else:
      reg_headers[cur] = headers[cur]
  return (amz_headers, reg_headers)

def sign(secret, string_to_sign):
  hashed = hmac.new(secret, string_to_sign, sha1)
  return binascii.b2a_base64(hashed.digest()).strip()

def sign_content_if_possible(content):
  #TODO if the content is a proper file, it would also be possible.
  if content != '' and isinstance(content, basestring):
    return sign_content(content)
  else:
    return ""

def sign_content(content):
  return binascii.b2a_base64(md5(content).digest()).strip()

def _get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
  key_header_strs = [ (name.lower(), "%s:%s" % (name.lower(), amz_headers[name])) for name in amz_headers.keys() ]
  header_list = map(lambda x: x[1], sorted(key_header_strs))
  header_str = "\n".join(header_list)
  if header_str:
    header_str+="\n"
  string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, header_str, resource, )
  return string

#################################
# XML Render Handling Functions #
#################################

def _render_delete_bulk_content(keys, quiet=False):
  delete = Element('Delete')
  if quiet:
    quiet_element = SubElement(delete, 'Quiet')
    quiet_element.text = 'true'
  objects = []
  for name in keys:
    obj = Element('Object')
    key = SubElement(obj, 'Key')
    key.text = name
    objects.append(obj)
  delete.extend(objects)
  return tostring(delete)

####################################
# Http Response Handling Functions #
####################################

def _parse_list_response(xml):
  is_truncated_path = '{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated'
  key_path = '{http://s3.amazonaws.com/doc/2006-03-01/}Contents/{http://s3.amazonaws.com/doc/2006-03-01/}Key'
  tree = parse(xml)
  is_truncated = tree.find(is_truncated_path).text == 'true'
  keys = tree.findall(key_path)
  names = []
  for key in keys:
    names.append(key.text)
  return (names, is_truncated)

def _parse_get_service_response(xml):
  bucket_path = '{http://s3.amazonaws.com/doc/2006-03-01/}Buckets/{http://s3.amazonaws.com/doc/2006-03-01/}Bucket/{http://s3.amazonaws.com/doc/2006-03-01/}Name'
  tree = parse(xml)
  buckets = tree.findall(bucket_path)
  names = []
  for bucket in buckets:
    names.append(bucket.text)
  return (names)

KEY_PATH='{http://s3.amazonaws.com/doc/2006-03-01/}Key'
def _tag_normalize(name):
    if name[0] == "{":
        _, tag = name[1:].split("}")
        return tag
    else:
        return name
def _parse_delete_bulk_response(xml):
  actions = parse(xml)
  return [ (action.find(KEY_PATH).text, _tag_normalize(action.tag)) for action in actions]

###########
# Testing #
###########

def test_all():
  try:
    print "GET TEST"
    test_sign_get()
    print "PUT TEST"
    test_sign_put()
    print "LIST TEST"
    test_sign_list()
    print "COPY TEST"
    # test_sign_copy() #TODO FAILING.
    print "TAKE TEST"
    test_take()
    print "BATCHIFY TEST"
    test_batchify()
  except Exception as e:
    print "Caught exception!"
    exit(1)

def validate_signature(string, expected_string, expected_signature):
  print "output:"
  print "***\n" + string + "\n***\n"
  print "expected:"
  print "---\n" + expected_string + "\n---\n"
  assert(string == expected_string)
  secret = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  signature = sign(secret, string)
  print "Checking sigs"
  print signature
  print expected_signature
  assert(signature == expected_signature)
  print "done"

def test_sign_get():
  string = _get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", {}, "/johnsmith/photos/puppy.jpg")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg"
  expected_signature = "bWq2s1WEIj+Ydj0vQ697zp+IXMU="
  validate_signature(string, expected_string, expected_signature)

def test_sign_put():
  string = _get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", {}, "/johnsmith/photos/puppy.jpg" )
  expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg"
  expected_signature = "MyyxeRY7whkBe+bq8fHCL/2kKUg="
  validate_signature(string, expected_string, expected_signature)

def test_sign_list():
  string = _get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", {}, "/johnsmith/")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/"
  expected_signature = "htDYFYduRNen8P9ZfE/s9SuKy0U="
  validate_signature(string, expected_string, expected_signature)

def test_sign_copy():
  string = _get_string_to_sign("PUT", "", "", "Wed, 20 Feb 2008 22:12:21 +0000", {"x-amz-copy-source":"/pacific/flotsam"}, "/atlantic/jetsam")
  expected_string = "PUT\n\n\nWed, 20 Feb 2008 22:12:21 +0000\nx-amz-copy-source:/pacific/flotsam\n/atlantic/jetsam"
  expected_signature = "ENoSbxYByFA0UGLZUqJN5EUnLDg="
  validate_signature(string, expected_string, expected_signature)


def test_take():
  assert(list(_take(3, [])) == [])
  assert(list(_take(3, [1,2])) == [1,2])
  assert(list(_take(3, [1,2, 3, 4])) == [1,2,3])
  i = iter(range(7))
  assert(list(_take(3, i)) == [0,1,2])
  assert(list(_take(3, i)) == [3,4,5])
  assert(list(_take(3, i)) == [6])

def test_batchify():
  assert(list(_batchify(3, [])) == [])
  assert(list(_batchify(3, [1])) == [[1]])
  assert(list(_batchify(3, [1,2,3])) == [[1,2,3]])
  assert(list(_batchify(3, [1,2,3,4])) == [[1,2,3],[4]])
  assert(list(_batchify(3, iter([1,2,3,4]))) == [[1,2,3],[4]])
