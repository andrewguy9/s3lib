import s3lib
from s3lib import *
import pytest
import re

def validate_signature(string, expected_string, expected_signature):
  assert(string == expected_string)
  secret = b'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  signature = sign(secret, string)
  assert(signature == expected_signature)

def test_sign_get():
  string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", {}, "/johnsmith/photos/puppy.jpg")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg".encode('utf-8')
  expected_signature = b"bWq2s1WEIj+Ydj0vQ697zp+IXMU="
  validate_signature(string, expected_string, expected_signature)

def test_sign_put():
  string = get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", {}, "/johnsmith/photos/puppy.jpg" )
  expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg".encode('utf-8')
  expected_signature = b"MyyxeRY7whkBe+bq8fHCL/2kKUg="
  validate_signature(string, expected_string, expected_signature)

def test_sign_list():
  string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", {}, "/johnsmith/")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/".encode('utf-8')
  expected_signature = b"htDYFYduRNen8P9ZfE/s9SuKy0U="
  validate_signature(string, expected_string, expected_signature)

@pytest.mark.skip()
def test_sign_copy():
  string = get_string_to_sign("PUT", "", "", "Wed, 20 Feb 2008 22:12:21 +0000", {"x-amz-copy-source":"/pacific/flotsam"}, "/atlantic/jetsam")
  expected_string = "PUT\n\n\nWed, 20 Feb 2008 22:12:21 +0000\nx-amz-copy-source:/pacific/flotsam\n/atlantic/jetsam".encode('utf-8')
  expected_signature = b"ENoSbxYByFA0UGLZUqJN5EUnLDg="
  validate_signature(string, expected_string, expected_signature)

def test_s3_request_arg():
  assert s3lib._calculate_query_arg_str({}) == ""
  assert s3lib._calculate_query_arg_str({'k':None}) == "?k"
  assert s3lib._calculate_query_arg_str({'k':'v'}) == "?k=v"
  assert s3lib._calculate_query_arg_str({'k':'v', 'f':None}) == "?f&k=v"
  two_args = s3lib._calculate_query_arg_str({'k1':'v1', 'k2':'v2'}) 
  assert re.findall("k2=v2", two_args) and re.findall("k1=v1", two_args)
  # Test url-encoding.
  assert s3lib._calculate_query_arg_str({"b@dkey": "b@dvalue$$"}) == "?b%40dkey=b%40dvalue%24%24"
  assert s3lib._calculate_query_arg_str({"b@dkey": None}) == "?b%40dkey"


def test_s3_get_object_url():
    """
    Example from https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
    """
    #TODO Upgrade to new style URLs.
    expected = "https://s3.amazonaws.com/jbarr-public/images/ritchie_and_thompson_pdp11.jpeg"
    bucket = "jbarr-public"
    key = "images/ritchie_and_thompson_pdp11.jpeg"
    conn = s3lib.Connection("someaccess", b"somesecret")
    url = conn.get_object_url(bucket, key)
    assert url == expected
