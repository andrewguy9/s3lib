############################
# Python Iteration Helpers #
############################

def take(size, collection):
  if size < 1:
    raise ValueError("Size must be 1 or greater")
  for _, elem in zip(range(size), collection):
    yield elem

def batchify(size, collection):
  if size < 1:
    raise ValueError("Size must be 1 or greater")
  iterator = iter(collection)
  while True:
    batch = list(take(size, iterator))
    if len(batch) == 0:
      break
    else:
      yield batch

##########################
# Signing Util Functions #
##########################

def split_headers(headers):
  """Some headers are special to amazon. Splits those from regular http headers"""
  amz_headers = {}
  reg_headers = {}
  for cur in headers:
    if cur.lower().startswith('x-amz-'):
      amz_headers[cur] = headers[cur]
    else:
      reg_headers[cur] = headers[cur]
  return (amz_headers, reg_headers)

subresources = ["versioning", "location", "acl", "torrent", "lifecycle", "versionid", "delete"]
def split_args(args):
  return {subresource:args[subresource]
          for subresource in subresources if subresource in args}

def get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
  """
  method is str.
  content_md5 is ?
  content_type is str
  http_date is str
  amz_headers is dict string string
  resource is ?
  returns utf-8 encoded bytes.
  """
  key_header_strs = [ (name.lower(), "%s:%s" % (name.lower(), amz_headers[name])) for name in list(amz_headers.keys()) ]
  header_list = [x[1] for x in sorted(key_header_strs)]
  header_str = "\n".join(header_list)
  if header_str:
    header_str+="\n"
  string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, header_str, resource, )
  return string.encode('utf-8')

def raise_http_resp_error(resp):
    message = "S3 request failed with:\n%s %s\n%s\n%s" % (resp.status, resp.reason, resp.msg, resp.read())
    raise ValueError(message)

