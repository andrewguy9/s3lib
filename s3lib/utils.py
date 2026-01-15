import logging
from http.client import HTTPResponse
from typing import Iterable, TypeVar

# Configure module-level logger
logger = logging.getLogger(__name__)

X = TypeVar('X')

############################
# Python Iteration Helpers #
############################

def take(size: int, collection: Iterable[X]) -> Iterable[X]:
  if size < 1:
    raise ValueError("Size must be 1 or greater")
  for _, elem in zip(range(size), collection):
    yield elem

def batchify(size: int, collection: Iterable[X]) -> Iterable[Iterable[X]]:
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

def split_headers(headers: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
  """
  Amazon has special http headers which have the prefix 'x-amz-'.
  These headers need to be collected and used in signature calculation
  for authentication.

  split_headers returns a two element tuple with the headers split into
  (amz_headers, reg_headers).
  """
  amz_headers = {}
  reg_headers = {}
  for cur in headers:
    if cur.lower().startswith('x-amz-'):
      amz_headers[cur] = headers[cur]
    else:
      reg_headers[cur] = headers[cur]
  return (amz_headers, reg_headers)

subresources = ["versioning", "location", "acl", "torrent", "lifecycle", "versionid", "delete"]
def split_args(args: dict[str, str]) -> dict[str, str]:
  """
  split_args filters the input dictionary of query parameters to only include
  those that are considered subresources by Amazon S3.
  """
  return {subresource:args[subresource]
          for subresource in subresources if subresource in args}

def get_string_to_sign(
    method: str,
    content_md5: str,
    content_type: str,
    http_date: str,
    amz_headers: dict[str, str],
    resource: str) -> bytes:
  """
  S3 signature calculation requires all of these parameters and slams them together to create a canonical string to be signed.
  This string is encoded as a UTF-8 byte string.
  """
  key_header_strs = [ (name.lower(), "%s:%s" % (name.lower(), amz_headers[name])) for name in list(amz_headers.keys()) ]
  header_list = [x[1] for x in sorted(key_header_strs)]
  header_str = "\n".join(header_list)
  if header_str:
    header_str+="\n"
  string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, header_str, resource, )
  return string.encode('utf-8')

def raise_http_resp_error(resp: HTTPResponse) -> None:
    body = resp.read()
    message = "S3 request failed with:\n%s %s\n%s\n%s" % (resp.status, resp.reason, resp.msg, body.decode('utf-8'))

    # Log error details if debug mode is enabled
    logger.debug("S3 HTTP Error %s %s", resp.status, resp.reason)
    logger.debug("Response headers: %s", dict(resp.getheaders()))
    if body:
        logger.debug("Response body: %s", body.decode('utf-8', errors='replace'))

    raise ValueError(message)

