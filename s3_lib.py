#!/usr/bin/env python

import hmac
from hashlib import sha1
import binascii
import httplib 
import time

def sign_string(key, string_to_sign):
        hashed = hmac.new(key, string_to_sign, sha1)
        return binascii.b2a_base64(hashed.digest()).strip()

def get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
    assert(amz_headers == [])
    if amz_headers == []:
        amz_headers = ""
    string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, amz_headers, resource, )
    return string

def s3_list_request(bucket, key, host, port, access_id, secret, marker=None, max_keys=None):
    s3_request(bucket, key, host, port, access_id, secret, {'marker':marker, 'max-keys':max_keys})

def s3_head_request(bucket, key, host, port, access_id, secret):
    (status, headers, data) = s3_request(bucket, key, host, port, access_id, secret, {})
    print headers

def s3_request(bucket, key, host, port, access_id, secret, args):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S +0000', time.gmtime())

    args_str = "&".join(args)
    if args_str:
        args_str = "?" + args_str
    canonical_resource = "/%s/%s" % (bucket, key)
    resource = "/" + key + args_str
    
    method = "GET"
    content_type = ""
    content_md5 = ""
    amz_headers = []
    string_to_sign = get_string_to_sign(method, content_md5, content_type, http_now, amz_headers, canonical_resource)
    signature = sign_string(secret, string_to_sign)
    # print "string to sign:"
    # print string_to_sign
    # print "signature: ", signature

    headers = {}
    headers["Host"] = "%s.%s" % (bucket, host)
    headers["Date"] = http_now
    headers["Authorization"] = "AWS %s:%s" % (access_id, signature)
    if content_type:
        headers["Content-Type"] = content_type

    conn = httplib.HTTPConnection(host, port)
    conn.request(method, resource, "", headers)
    resp = conn.getresponse()
    if resp.status != httplib.OK:
        raise ValueError("S3 request failed with: %s" % (resp.status))
    return (resp.status, resp.getheaders(), resp.read())

###########
# Testing #
###########

def validate_signature(string, expected_string, expected_signature):
    print "testing..."
    print "***\n" + string + "\n***\n"
    print "---\n" + expected_string + "\n---\n"
    assert(string == expected_string)
    secret = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    signature = sign_string(secret, string)
    print "Checking sigs"
    print signature
    print expected_signature
    assert(signature == expected_signature)
    print "done"

def test_sign_1():
    string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", [], "/johnsmith/photos/puppy.jpg")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg"
    expected_signature = "bWq2s1WEIj+Ydj0vQ697zp+IXMU="
    validate_signature(string, expected_string, expected_signature)

def test_sign_2():
    string = get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", [], "/johnsmith/photos/puppy.jpg" )
    expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg"
    expected_signature = "MyyxeRY7whkBe+bq8fHCL/2kKUg="
    validate_signature(string, expected_string, expected_signature)

def test_sign_3():
    string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", [], "/johnsmith/")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/"
    expected_signature = "htDYFYduRNen8P9ZfE/s9SuKy0U="
    validate_signature(string, expected_string, expected_signature)


