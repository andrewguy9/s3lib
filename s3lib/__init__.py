#!/usr/bin/env python

import hmac
from hashlib import sha1
import binascii
import httplib
import time
from xml.etree.ElementTree import fromstring as parse

class Connection:

  def __init__(self, access_id, secret, host=None, port=None):
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

  #######################
  # Interface Functions #
  #######################
  def list_bucket(self, bucket, start, prefix, batch_size):
    more = True
    while more:
      xml = self._s3_list_request(bucket, start, prefix, batch_size)
      keys, truncated = _parse_list_response(xml)
      for key in keys:
        yield key
      start = key # Next request should start from last request's last item.
      more = truncated

  def get_object(self, bucket, key):
    return self._s3_get_request(bucket, key)

  def head_object(self, bucket, key):
    status, headers = self._s3_head_request(bucket, key)
    return headers

  def delete_object(self, bucket, key):
    status, headers = self._s3_delete_request(bucket, key)
    return (status, headers)

  def copy_object(self, src_bucket, src_key, dst_bucket, dst_key, headers):
    status, headers, xml = self._s3_copy_request(src_bucket, src_key, dst_bucket, dst_key, headers)
    return (status, headers, xml)

  def put_object(self, bucket, key, data, headers):
    (status, headers, xml) = self._s3_put_request(bucket, key, data, headers)
    return (status, headers, xml)

##########################
# Http request Functions #
##########################

  def _s3_list_request(self, bucket, marker=None, prefix=None, max_keys=None):
    args = {}
    if marker:
      args['marker'] = marker
    if prefix:
      args['prefix'] = prefix
    if max_keys:
      args['max-keys'] = max_keys
    (status, headers, xml) = self._s3_request("GET", bucket, "", 30, args, {}, '')
    if status != httplib.OK:
      raise ValueError("S3 request failed with: %s" % (status))
    else:
      return xml

  def _s3_get_request(self, bucket, key):
    (status, headers, response) = self._s3_request("GET", bucket, key, 2, {}, {}, '')
    if status != httplib.OK:
      raise ValueError("S3 request failed with %s" % (status))
    else:
      return response

  def _s3_head_request(self, bucket, key):
    (status, headers, response) = self._s3_request("HEAD", bucket, key, 2, {}, {}, '')
    if status != httplib.OK:
      raise ValueError("S3 request failed with %s" % (status))
    else:
      return (status, headers)

  def _s3_delete_request(self, bucket, key):
    (status, headers, response) = self._s3_request("DELETE", bucket, key, 2, {}, {}, '')
    if status != httplib.NO_CONTENT:
      raise ValueError("S3 request failed with %s" % (status))
    else:
      return (status, headers)

  def _s3_copy_request(self, src_bucket, src_key, dst_bucket, dst_key, headers):
    copy_headers = {'x-amz-copy-source':"/%s/%s" % (src_bucket, src_key)}
    copy_headers['x-amz-metadata-directive'] = 'REPLACE'
    headers = dict(headers.items() + copy_headers.items())
    (status, resp_headers, response) = self._s3_request("PUT", dst_bucket, dst_key, 5, {}, headers, '')
    if status != httplib.OK:
      raise ValueError("S3 request failed with: %s" % (status))
    else:
      return (status, resp_headers, response)

  def _s3_put_request(self, bucket, key, data, headers):
    args = {}
    (status, resp_headers, response) = self._s3_request("PUT", bucket, key, 5, args, headers, data)
    if status != httplib.OK:
      raise ValueError("S3 request failed with: %s" % (status))
    else:
      return (status, resp_headers, response)

  def _s3_request(self, method, bucket, key, timeout, args, headers, content):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S +0000', time.gmtime())

    args = map( lambda x: "=".join(x), args.items())
    args_str = "&".join(args)
    if args_str:
      args_str = "?" + args_str
    canonical_resource = "/%s/%s" % (bucket, key)
    resource = "/" + key + args_str

    try:
      content_type = headers['Content-Type']
    except KeyError:
      content_type = ''
    content_md5 = "" #TODO fix this when you really support upload.
    (amz_headers, reg_headers) = _split_headers(headers)
    string_to_sign = _get_string_to_sign(method, content_md5, content_type, http_now, amz_headers, canonical_resource)
    signature = sign(self.secret, string_to_sign)

    headers["Host"] = "%s.%s" % (bucket, self.host)
    headers["Date"] = http_now
    headers["Authorization"] = "AWS %s:%s" % (self.access_id, signature)

    conn = httplib.HTTPConnection(self.host, self.port, timeout=timeout)
    conn.request(method, resource, content, headers)
    resp = conn.getresponse()
    data = resp.read() #TODO MIGHT NOT WANT TO READ WHOLE KEY.
    conn.close()
    return (resp.status, resp.getheaders(), data)

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

def _get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
  key_header_strs = [ (name.lower(), "%s:%s" % (name.lower(), amz_headers[name])) for name in amz_headers.keys() ]
  header_list = map(lambda x: x[1], sorted(key_header_strs))
  header_str = "\n".join(header_list)
  if header_str:
    header_str+="\n"
  string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, header_str, resource, )
  return string

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
    test_sign_copy()
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

