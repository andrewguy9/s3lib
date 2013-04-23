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

def copy_object(src_bucket, src_key, dst_bucket, dst_key, host, port, headers, access_id, secret):
    (status, headers, xml) = s3_copy_request(src_bucket, src_key, dst_bucket, dst_key, host, port, headers, access_id, secret) 
    return (status, headers, xml)

def put_object(bucket, key, data, host, port, headers, access_id, secret):
    (status, headers, xml) = s3_put_request(bucket, key, data, host, port, headers, access_id, secret)
    return (status, headers, xml)


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
    (status, headers, xml) = s3_request("GET", bucket, "", host, port, access_id, secret, args, {}, '')
    if status != httplib.OK:
        raise ValueError("S3 request failed with: %s" % (status))
    else:
        return xml

def s3_head_request(bucket, key, host, port, access_id, secret):
    (status, headers, response) = s3_request("HEAD", bucket, key, host, port, access_id, secret, {}, {}, '')
    if status != httplib.OK:
        raise ValueError("S3 request failed with %s" % (status))
    else:
        return (status, headers)

def s3_copy_request(src_bucket, src_key, dst_bucket, dst_key, host, port, headers, access_id, secret):
    copy_headers = {'x-amz-copy-source':"/%s/%s" % (src_bucket, src_key)}
    copy_headers['x-amz-metadata-directive'] = 'REPLACE'
    headers = dict(headers.items() + copy_headers.items())
    (status, resp_headers, response) = s3_request("PUT", dst_bucket, dst_key, host, port, access_id, secret, {}, headers, '')
    return (status, resp_headers, response)

def s3_put_request(bucket, key, data, host, port, headers, access_id, secret):
    args = {}
    (status, headers, response) = s3_request("PUT", bucket, key, host, port, access_id, secret, args, headers, data)
    return (status, headers, response)


def s3_request(method, bucket, key, host, port, access_id, secret, args, headers, content):
    http_now = time.strftime('%a, %d %b %G %H:%M:%S +0000', time.gmtime())

    args = map( lambda x: "=".join(x), args.items())
    args_str = "&".join(args)
    if args_str:
        args_str = "?" + args_str
    canonical_resource = "/%s/%s" % (bucket, key)
    resource = "/" + key + args_str
    
    content_type = ""
    content_md5 = ""
    (amz_headers, reg_headers) = split_headers(headers)
    string_to_sign = get_string_to_sign(method, content_md5, content_type, http_now, amz_headers, canonical_resource)
    signature = sign_string(secret, string_to_sign)

    headers["Host"] = "%s.%s" % (bucket, host)
    headers["Date"] = http_now
    headers["Authorization"] = "AWS %s:%s" % (access_id, signature)
    if content_type:
        headers["Content-Type"] = content_type

    print "\nHEADERS\n", headers
    for header, value in headers.items():
        print header,":",value
    print "HEADERS END\n\n\n"

    conn = httplib.HTTPConnection(host, port)
    conn.request(method, resource, content, headers)
    resp = conn.getresponse()
    return (resp.status, resp.getheaders(), resp.read())

##########################
# Signing Util Functions #
##########################

amz_headers_whitelist = ["Cache-Control", "Content-Disposition", "Content-Type", "Content-Language", "Expires", "Content-Encoding"]
def split_headers(headers):
    """Some headers are special to amazon. Splits those from regular http headers"""
    amz_headers = {}
    reg_headers = {}
    for cur in headers:
        if cur in amz_headers_whitelist:
            amz_headers[cur] = headers[cur]
        elif cur.lower().startswith('x-amz-'):
            amz_headers[cur] = headers[cur]
        else:
            reg_headers[cur] = headers[cur]
    return (amz_headers, reg_headers)

def sign_string(key, string_to_sign):
    hashed = hmac.new(key, string_to_sign, sha1)
    return binascii.b2a_base64(hashed.digest()).strip()

def get_string_to_sign(method, content_md5, content_type, http_date, amz_headers, resource):
    key_header_strs = [ (name.lower(), "%s:%s" % (name.lower(), amz_headers[name])) for name in amz_headers.keys() ]
    header_list = map(lambda x: x[1], sorted(key_header_strs))
    header_str = "\n".join(header_list)
    if header_str:
        header_str+="\n"
    string = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, http_date, header_str, resource, )
    print "string to sign\n", string, "\nstring to sign end\n\n\n"
    return string

###########
# Testing #
###########

def test_all():
    print "GET TEST"
    test_sign_get()
    print "PUT TEST"
    test_sign_put()
    print "LIST TEST"
    test_sign_list()
    print "COPY TEST"
    test_sign_copy()

def validate_signature(string, expected_string, expected_signature):
    print "output:"
    print "***\n" + string + "\n***\n"
    print "expected:"
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
    string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", {}, "/johnsmith/photos/puppy.jpg")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg"
    expected_signature = "bWq2s1WEIj+Ydj0vQ697zp+IXMU="
    validate_signature(string, expected_string, expected_signature)

def test_sign_put():
    string = get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", {}, "/johnsmith/photos/puppy.jpg" )
    expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg"
    expected_signature = "MyyxeRY7whkBe+bq8fHCL/2kKUg="
    validate_signature(string, expected_string, expected_signature)

def test_sign_list():
    string = get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", {}, "/johnsmith/")
    expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/"
    expected_signature = "htDYFYduRNen8P9ZfE/s9SuKy0U="
    validate_signature(string, expected_string, expected_signature)

def test_sign_copy():
    string = get_string_to_sign("PUT", "", "", "Wed, 20 Feb 2008 22:12:21 +0000", {"x-amz-copy-source":"/pacific/flotsam"}, "/atlantic/jetsam")
    expected_string = "PUT\n\n\nWed, 20 Feb 2008 22:12:21 +0000\nx-amz-copy-source:/pacific/flotsam\n/atlantic/jetsam"
    expected_signature = "ENoSbxYByFA0UGLZUqJN5EUnLDg="
    validate_signature(string, expected_string, expected_signature)

