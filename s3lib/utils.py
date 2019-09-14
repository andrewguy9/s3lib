############################
# Python Iteration Helpers #
############################

def take(size, collection):
  if size < 1:
    raise ValueError("Size must be 1 or greater")
  iterator = iter(collection)
  """Yields up to size elements from iterator."""
  for i in range(size):
    yield next(iterator)

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

def get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
  key_header_strs = [ (name.lower(), "%s:%s" % (name.lower(), amz_headers[name])) for name in list(amz_headers.keys()) ]
  header_list = [x[1] for x in sorted(key_header_strs)]
  header_str = "\n".join(header_list)
  if header_str:
    header_str+="\n"
  string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, header_str, resource, )
  return string

