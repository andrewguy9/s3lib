#!/usr/bin/env python

import httplib 
from hashlib import sha1
import hmac
import binascii
import argparse
import time
from os.path import expanduser

parser = argparse.ArgumentParser("Program lists all the objects in an s3 bucket. Works on really big buckets")

parser.add_argument('--host', type=str, dest='host', action='store', default='s3.amazonaws.com', help='Name of host')
parser.add_argument('--port', type=int, dest='port', action='store', default=80, help='Port to connect to')
parser.add_argument('--bucket', type=str, dest='bucket', action='store', required=True, help='Name of bucket')
parser.add_argument('--output', type=str, dest='output', action='store', default='/dev/stdout', help='Name of output')
parser.add_argument('--creds', type=str, dest='creds', action='store', default=expanduser("~/.s3"), help='Name of file to find aws access id and secret key')

def main():
    args = parser.parse_args()
    (access_id, secret_key) = load_creds(args.creds)
    fetch_bucket_part(args.bucket, args.host, args.port, access_id, secret_key)

def load_creds(path):
    with open(path, "r") as f:
        access_id = f.readline().strip()
        secret_key = f.readline().strip()
        return (access_id, secret_key)

def sign_string(key, string_to_sign):
        hashed = hmac.new(key, string_to_sign, sha1)
        return binascii.b2a_base64(hashed.digest()).strip()

def get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
    assert(amz_headers == [])
    if amz_headers == []:
        amz_headers = ""
    string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, amz_headers, resource, )
    return string

def fetch_bucket_part(bucket, host, port, access_id, secret, start=None, max_items=None):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S +0000', time.gmtime())

    args = []
    if start:
        args.append( "marker=%s" % (start) )
    if max_items:
        args.append( "max-keys=%s" % (max_items) )
    args_str = ""
    if args:
        args = "?" + "&".join(args)
    canonical_resource = "/%s/" % (bucket)
    resource = "/" + args_str
    
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
    headers["Host"]          = "%s.%s" % (bucket, host)
    headers["Date"]          = http_now
    headers["Authorization"] = "AWS %s:%s" % (access_id, signature)
    if content_type:
        headers["Content-Type"]  = content_type

    conn = httplib.HTTPConnection(host, port)
    conn.request(method, resource, "", headers)
    resp = conn.getresponse()
    # print resp.status
    # print resp.reason
    # print resp.getheaders()
    print resp.read()

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

if __name__ == '__main__':
    # test_sign_1()
    # test_sign_2()
    # test_sign_3()
    main()



