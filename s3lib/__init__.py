#!/usr/bin/env python

import hmac
from hashlib import sha1
from hashlib import md5
import binascii
import http.client
import time
from xml.etree.ElementTree import fromstring as parse
from xml.etree.ElementTree import Element, SubElement, tostring
from s3lib.utils import split_headers, batchify, take, get_string_to_sign, raise_http_resp_error

class Connection:

  ############################
  # Python special functions #
  ############################
  def __init__(self, access_id, secret, host=None, port=None, conn_timeout=None):
    """
    access_id is ?
    secret is bytes
    host is maybe str
    port is maybe int
    conn_timeout is maybe int seconds
    """
    assert isinstance(secret, bytes)
    self.access_id = access_id
    self.secret = secret
    if port is None:
      self.port = 80
    else:
      self.port = port
    if host is None:
      self.host = "s3.amazonaws.com" #TODO support multi-regions
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

  #TODO MOVE
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

  #TODO MOVE
  def get_object(self, bucket, key):
    """ pull down bucket object by key """
    #TODO Want to replace with some enter, exit struct.
    return self._s3_get_request(bucket, key)

  #TODO MOVE
  def head_object(self, bucket, key):
    """ get request metadata for key in bucket """
    status, headers = self._s3_head_request(bucket, key)
    return headers

  #TODO MOVE
  def delete_object(self, bucket, key):
    """ delete key from bucket """
    status, headers = self._s3_delete_request(bucket, key)
    return (status, headers)

  #TODO MOVE
  def delete_objects(self, bucket, keys, batch_size, quiet):
    """ delete keys from bucket """
    for batch in batchify(batch_size, keys):
      xml = self._s3_delete_bulk_request(bucket, batch, quiet)
      results = _parse_delete_bulk_response(xml)
      for (key, result) in results:
        yield key, result

  #TODO MOVE, need rewrite
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
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.

  #TODO move
  def _s3_list_request(self, bucket, marker=None, prefix=None, max_keys=None):
    args = {}
    if marker:
      args['marker'] = marker
    if prefix:
      args['prefix'] = prefix
    if max_keys:
      args['max-keys'] = max_keys
    resp = self._s3_request("GET", bucket, "", args, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.

  #TODO move
  def _s3_get_request(self, bucket, key):
    resp = self._s3_request("GET", bucket, key, {}, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return resp

  #TODO move
  def _s3_head_request(self, bucket, key):
    resp = self._s3_request("HEAD", bucket, key, {}, {}, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero size response. Required to reset the connection.
    return (resp.status, resp.getheaders())

  #TODO move
  def _s3_delete_request(self, bucket, key):
    resp = self._s3_request("DELETE", bucket, key, {}, {}, '')
    if resp.status != http.client.NO_CONTENT:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero size response. Required to reset the connection
    return (resp.status, resp.getheaders())

  #TODO move
  def _s3_delete_bulk_request(self, bucket, keys, quiet):
    content = _render_delete_bulk_content(keys, quiet)
    resp = self._s3_request("POST", bucket, "/?delete", {}, {}, content)
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    results = resp.read() #TODO HAS A PAYLOAD, MAYBE NOT BEST READ CANDIDATE.
    return results

  #TODO move
  def _s3_copy_request(self, src_bucket, src_key, dst_bucket, dst_key, headers):
    copy_headers = {'x-amz-copy-source':"/%s/%s" % (src_bucket, src_key)}
    copy_headers['x-amz-metadata-directive'] = 'REPLACE'
    headers = dict(list(headers.items()) + list(copy_headers.items()))
    resp = self._s3_request("PUT", dst_bucket, dst_key, {}, headers, '')
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    return (resp.status, resp.getheaders())

  #TODO move
  def _s3_put_request(self, bucket, key, data, headers):
    args = {}
    resp = self._s3_request("PUT", bucket, key, args, headers, data)
    if resp.status != http.client.OK:
      raise_http_resp_error(resp)
    resp.read() #NOTE: Should be zero length response. Required to reset the connection.
    return (resp.status, resp.getheaders())

  #TODO move
  def _s3_request(self, method, bucket, key, args, headers, content):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S +0000', time.gmtime())

    canonical_resource = "/"
    if bucket:
      canonical_resource += bucket + "/"
      if key:
        canonical_resource += key

    resource = "/"
    if key:
      resource += key
    resource += _calculate_query_arg_str(args)

    try:
      content_type = headers['Content-Type']
    except KeyError:
      content_type = ''
    content_md5 = sign_content_if_possible(content)
    (amz_headers, reg_headers) = split_headers(headers)
    string_to_sign = get_string_to_sign(method, content_md5, content_type, http_now, amz_headers, canonical_resource)
    signature = sign(self.secret, string_to_sign)

    if bucket:
      host = bucket + "." + self.host
    else:
      host = self.host
    headers["Host"] = host
    headers["Date"] = http_now
    headers["Authorization"] = "AWS %s:%s" % (self.access_id, signature.decode('ascii'))
    headers["Connection"] = "keep-alive"
    if content_md5 != '':
      headers['Content-MD5'] = content_md5

    self.conn.request(method, resource, content, headers)
    resp = self.conn.getresponse()
    return resp

###########################
# S3 Connection Functions #
###########################
  #TODO refactor could fix issues.
  def _connect(self):
    self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.conn_timeout)

  def _disconnect(self):
    self.conn.close()

def sign(secret, string_to_sign):
  """
  secret is a str?
  string_to_sign is a str.
  return bytes signature.
  """
  hashed = hmac.new(secret, string_to_sign, sha1)
  return binascii.b2a_base64(hashed.digest()).strip()

def sign_content_if_possible(content):
  #TODO if the content is a proper file, it would also be possible.
  if content != '' and isinstance(content, str):
    return sign_content(content)
  else:
    return ""

def sign_content(content):
  return binascii.b2a_base64(md5(content).digest()).strip()

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

#TODO might change if we upgrade bucket list call.
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

def _calculate_query_arg_str(args):
  """
  args is dict of str-> Maybe str.
  Produces a query arg string like "/?flag_name&argName=argValue..."
  always returns a string. If no args are present produces the empty string.
  """
  value_args = ["%s=%s"%(arg, value) for (arg, value) in list(args.items()) if value is not None]
  flag_args = ["%s"%arg for (arg, value) in list(args.items()) if value is None]
  args_str = "&".join(flag_args+value_args)
  if args_str:
    args_str = "?" + args_str
  return args_str

