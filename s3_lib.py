#!/usr/bin/env python

import hmac
from hashlib import sha1
import binascii
import httplib 
import time
from xml.etree.ElementTree import fromstring as parse

##############################
# Python interface functinos #
##############################

def list_bucket(bucket, host, port, access_id, secret, start, max_items):
    #TODO handle max_items
    more = True
    while more:
        xml = s3_list_request(bucket, host, port, access_id, secret, start, max_items)
        keys, truncated = parse_list_response(xml)
        for key in keys:
            yield key
        start = key # Next request should start from last request's last item.
        more = truncated

def head_object(bucket, key, host, port, access_id, secret):
    (status, headers) = s3_head_request(bucket, key, host, port, access_id, secret)
    return headers

####################################
# Http Response Handling Functions #
####################################

def parse_list_response(xml):
    is_truncated_path = '{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated'
    key_path = '{http://s3.amazonaws.com/doc/2006-03-01/}Contents/{http://s3.amazonaws.com/doc/2006-03-01/}Key'
    tree = parse(xml)
    is_truncated = tree.find(is_truncated_path).text == 'true'
    keys = tree.findall(key_path)
    names = []
    for key in keys:
        names.append(key.text)
    return (names, is_truncated)

##########################
# Http request Functions #
##########################

def s3_list_request(bucket, host, port, access_id, secret, marker=None, max_keys=None):
    args = {}
    if marker:
        args['marker'] = marker
    if max_keys:
        args['max-keys'] = max_keys
    (status, headers, xml) = s3_request("GET", bucket, "", host, port, access_id, secret, args)
    if status != httplib.OK:
        raise ValueError("S3 request failed with: %s" % (status))
    else:
        return xml

def s3_head_request(bucket, key, host, port, access_id, secret):
    (status, headers, response) = s3_request("HEAD", bucket, key, host, port, access_id, secret, {})
    if status != httplib.OK:
        raise ValueError("S3 request failed with %s" % (status))
    else:
        return (status, headers)

def s3_request(method, bucket, key, host, port, access_id, secret, args):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S +0000', time.gmtime())

    args = map( lambda x: "=".join(x), args.items())
    args_str = "&".join(args)
    if args_str:
        args_str = "?" + args_str
    canonical_resource = "/%s/%s" % (bucket, key)
    resource = "/" + key + args_str
    
    content_type = ""
    content_md5 = ""
    amz_headers = []
    string_to_sign = get_string_to_sign(method, content_md5, content_type, http_now, amz_headers, canonical_resource)
    signature = sign_string(secret, string_to_sign)

    headers = {}
    headers["Host"] = "%s.%s" % (bucket, host)
    headers["Date"] = http_now
    headers["Authorization"] = "AWS %s:%s" % (access_id, signature)
    if content_type:
        headers["Content-Type"] = content_type

    conn = httplib.HTTPConnection(host, port)
    conn.request(method, resource, "", headers)
    resp = conn.getresponse()
    return (resp.status, resp.getheaders(), resp.read())

##########################
# Signing Util Functions #
##########################

def sign_string(key, string_to_sign):
    hashed = hmac.new(key, string_to_sign, sha1)
    return binascii.b2a_base64(hashed.digest()).strip()

def get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
    assert(amz_headers == [])
    if amz_headers == []:
        amz_headers = "" #TODO I dont handle this yet.
    string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, amz_headers, resource, )
    return string

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

def test_sign_get():
    string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", [], "/johnsmith/photos/puppy.jpg")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg"
    expected_signature = "bWq2s1WEIj+Ydj0vQ697zp+IXMU="
    validate_signature(string, expected_string, expected_signature)

def test_sign_put():
    string = get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", [], "/johnsmith/photos/puppy.jpg" )
    expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg"
    expected_signature = "MyyxeRY7whkBe+bq8fHCL/2kKUg="
    validate_signature(string, expected_string, expected_signature)

def test_list():
    string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", [], "/johnsmith/")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/"
    expected_signature = "htDYFYduRNen8P9ZfE/s9SuKy0U="
    validate_signature(string, expected_string, expected_signature)

