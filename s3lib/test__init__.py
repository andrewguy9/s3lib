from s3lib import *
import pytest

def validate_signature(string, expected_string, expected_signature):
  assert(string == expected_string)
  secret = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  signature = sign(secret, string)
  assert(signature == expected_signature)

def test_sign_get():
  string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", {}, "/johnsmith/photos/puppy.jpg")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg"
  expected_signature = b"bWq2s1WEIj+Ydj0vQ697zp+IXMU="
  validate_signature(string, expected_string, expected_signature)

def test_sign_put():
  string = get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", {}, "/johnsmith/photos/puppy.jpg" )
  expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg"
  expected_signature = b"MyyxeRY7whkBe+bq8fHCL/2kKUg="
  validate_signature(string, expected_string, expected_signature)

def test_sign_list():
  string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", {}, "/johnsmith/")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/"
  expected_signature = b"htDYFYduRNen8P9ZfE/s9SuKy0U="
  validate_signature(string, expected_string, expected_signature)

@pytest.mark.skip()
def test_sign_copy():
  string = get_string_to_sign("PUT", "", "", "Wed, 20 Feb 2008 22:12:21 +0000", {"x-amz-copy-source":"/pacific/flotsam"}, "/atlantic/jetsam")
  expected_string = "PUT\n\n\nWed, 20 Feb 2008 22:12:21 +0000\nx-amz-copy-source:/pacific/flotsam\n/atlantic/jetsam"
  expected_signature = b"ENoSbxYByFA0UGLZUqJN5EUnLDg="
  validate_signature(string, expected_string, expected_signature)
