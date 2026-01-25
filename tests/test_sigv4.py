"""
Tests for AWS Signature Version 4 implementation.

Test cases derived from official AWS documentation:
https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

All test examples use the following credentials:
- Access Key ID: AKIAIOSFODNN7EXAMPLE
- Secret Access Key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
- Region: us-east-1
- Service: s3
- Timestamp: 20130524T000000Z
"""

import pytest
from s3lib.sigv4 import (
    create_canonical_request,
    create_string_to_sign,
    calculate_signature,
    derive_signing_key,
    sign_request_v4,
)


# Test credentials from AWS documentation
TEST_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
TEST_SECRET_KEY = b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
TEST_REGION = "us-east-1"
TEST_SERVICE = "s3"
TEST_TIMESTAMP = "20130524T000000Z"
TEST_DATE = "20130524"


class TestCanonicalRequest:
    """Test creation of canonical requests from various S3 operations."""

    def test_get_object_canonical_request(self):
        """
        Test Case 1: GET Object
        Example from AWS docs showing a GET request with Range header.
        """
        method = "GET"
        uri = "/test.txt"
        query_string = ""
        headers = {
            "host": "examplebucket.s3.amazonaws.com",
            "range": "bytes=0-9",
            "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "x-amz-date": "20130524T000000Z",
        }
        payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

        expected_canonical_request = (
            "GET\n"
            "/test.txt\n"
            "\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "range:bytes=0-9\n"
            "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            "x-amz-date:20130524T000000Z\n"
            "\n"
            "host;range;x-amz-content-sha256;x-amz-date\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

        canonical_request, signed_headers = create_canonical_request(
            method, uri, query_string, headers, payload_hash
        )

        assert canonical_request == expected_canonical_request
        assert signed_headers == "host;range;x-amz-content-sha256;x-amz-date"

    def test_put_object_canonical_request(self):
        """
        Test Case 2: PUT Object
        Example showing PUT with special characters in filename and payload.
        """
        method = "PUT"
        uri = "/test$file.text"
        query_string = ""
        headers = {
            "date": "Fri, 24 May 2013 00:00:00 GMT",
            "host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            "x-amz-date": "20130524T000000Z",
            "x-amz-storage-class": "REDUCED_REDUNDANCY",
        }
        payload_hash = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"

        expected_canonical_request = (
            "PUT\n"
            "/test%24file.text\n"
            "\n"
            "date:Fri, 24 May 2013 00:00:00 GMT\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\n"
            "x-amz-date:20130524T000000Z\n"
            "x-amz-storage-class:REDUCED_REDUNDANCY\n"
            "\n"
            "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class\n"
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        )

        canonical_request, signed_headers = create_canonical_request(
            method, uri, query_string, headers, payload_hash
        )

        assert canonical_request == expected_canonical_request
        assert signed_headers == "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"

    def test_get_bucket_lifecycle_canonical_request(self):
        """
        Test Case 3: GET Bucket Lifecycle
        Example showing query parameters for subresources.
        """
        method = "GET"
        uri = "/"
        query_string = "lifecycle="
        headers = {
            "host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "x-amz-date": "20130524T000000Z",
        }
        payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

        expected_canonical_request = (
            "GET\n"
            "/\n"
            "lifecycle=\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            "x-amz-date:20130524T000000Z\n"
            "\n"
            "host;x-amz-content-sha256;x-amz-date\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

        canonical_request, signed_headers = create_canonical_request(
            method, uri, query_string, headers, payload_hash
        )

        assert canonical_request == expected_canonical_request
        assert signed_headers == "host;x-amz-content-sha256;x-amz-date"

    def test_get_bucket_list_objects_canonical_request(self):
        """
        Test Case 4: GET Bucket (List Objects)
        Example showing query parameters with values.
        """
        method = "GET"
        uri = "/"
        query_string = "max-keys=2&prefix=J"
        headers = {
            "host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "x-amz-date": "20130524T000000Z",
        }
        payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

        expected_canonical_request = (
            "GET\n"
            "/\n"
            "max-keys=2&prefix=J\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            "x-amz-date:20130524T000000Z\n"
            "\n"
            "host;x-amz-content-sha256;x-amz-date\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

        canonical_request, signed_headers = create_canonical_request(
            method, uri, query_string, headers, payload_hash
        )

        assert canonical_request == expected_canonical_request
        assert signed_headers == "host;x-amz-content-sha256;x-amz-date"


class TestStringToSign:
    """Test creation of the string to sign."""

    def test_string_to_sign_for_get_object(self):
        """
        Test string to sign creation for GET object request.
        The canonical request hash for the GET object example.
        """
        canonical_request = (
            "GET\n"
            "/test.txt\n"
            "\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "range:bytes=0-9\n"
            "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            "x-amz-date:20130524T000000Z\n"
            "\n"
            "host;range;x-amz-content-sha256;x-amz-date\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

        # The SHA256 hash of the canonical request
        # This is what we expect to see in the string to sign
        expected_canonical_hash = "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"

        expected_string_to_sign = (
            "AWS4-HMAC-SHA256\n"
            "20130524T000000Z\n"
            "20130524/us-east-1/s3/aws4_request\n"
            f"{expected_canonical_hash}"
        )

        string_to_sign = create_string_to_sign(
            canonical_request,
            TEST_TIMESTAMP,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        assert string_to_sign == expected_string_to_sign


class TestSigningKey:
    """Test derivation of the signing key."""

    def test_derive_signing_key(self):
        """
        Test signing key derivation.
        The signing key is derived from the secret key, date, region, and service.
        """
        signing_key = derive_signing_key(
            TEST_SECRET_KEY,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        # The signing key is binary data (32 bytes for HMAC-SHA256)
        assert isinstance(signing_key, bytes)
        assert len(signing_key) == 32


class TestSignatureCalculation:
    """Test the complete signature calculation."""

    def test_get_object_signature(self):
        """
        Test Case 1: Complete signature for GET Object request.
        Expected signature from AWS documentation.
        """
        canonical_request = (
            "GET\n"
            "/test.txt\n"
            "\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "range:bytes=0-9\n"
            "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            "x-amz-date:20130524T000000Z\n"
            "\n"
            "host;range;x-amz-content-sha256;x-amz-date\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

        string_to_sign = create_string_to_sign(
            canonical_request,
            TEST_TIMESTAMP,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signing_key = derive_signing_key(
            TEST_SECRET_KEY,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signature = calculate_signature(signing_key, string_to_sign)

        expected_signature = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        assert signature == expected_signature

    def test_put_object_signature(self):
        """
        Test Case 2: Complete signature for PUT Object request.
        Expected signature from AWS documentation.
        """
        canonical_request = (
            "PUT\n"
            "/test%24file.text\n"
            "\n"
            "date:Fri, 24 May 2013 00:00:00 GMT\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\n"
            "x-amz-date:20130524T000000Z\n"
            "x-amz-storage-class:REDUCED_REDUNDANCY\n"
            "\n"
            "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class\n"
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        )

        string_to_sign = create_string_to_sign(
            canonical_request,
            TEST_TIMESTAMP,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signing_key = derive_signing_key(
            TEST_SECRET_KEY,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signature = calculate_signature(signing_key, string_to_sign)

        expected_signature = "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        assert signature == expected_signature

    def test_get_bucket_lifecycle_signature(self):
        """
        Test Case 3: Complete signature for GET Bucket Lifecycle request.
        Expected signature from AWS documentation.
        """
        canonical_request = (
            "GET\n"
            "/\n"
            "lifecycle=\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            "x-amz-date:20130524T000000Z\n"
            "\n"
            "host;x-amz-content-sha256;x-amz-date\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

        string_to_sign = create_string_to_sign(
            canonical_request,
            TEST_TIMESTAMP,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signing_key = derive_signing_key(
            TEST_SECRET_KEY,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signature = calculate_signature(signing_key, string_to_sign)

        expected_signature = "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        assert signature == expected_signature

    def test_get_bucket_list_objects_signature(self):
        """
        Test Case 4: Complete signature for GET Bucket (List Objects) request.
        Expected signature from AWS documentation.
        """
        canonical_request = (
            "GET\n"
            "/\n"
            "max-keys=2&prefix=J\n"
            "host:examplebucket.s3.amazonaws.com\n"
            "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            "x-amz-date:20130524T000000Z\n"
            "\n"
            "host;x-amz-content-sha256;x-amz-date\n"
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )

        string_to_sign = create_string_to_sign(
            canonical_request,
            TEST_TIMESTAMP,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signing_key = derive_signing_key(
            TEST_SECRET_KEY,
            TEST_DATE,
            TEST_REGION,
            TEST_SERVICE,
        )

        signature = calculate_signature(signing_key, string_to_sign)

        expected_signature = "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        assert signature == expected_signature


class TestCompleteSigningProcess:
    """Test the complete signing process using the wrapper function."""

    def test_sign_request_v4_get_object(self):
        """
        Test complete signing process for GET Object request.
        Verifies the Authorization header format and signature.
        """
        method = "GET"
        uri = "/test.txt"
        query_string = ""
        headers = {
            "host": "examplebucket.s3.amazonaws.com",
            "range": "bytes=0-9",
            "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "x-amz-date": "20130524T000000Z",
        }
        payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

        authorization_header = sign_request_v4(
            method,
            uri,
            query_string,
            headers,
            payload_hash,
            TEST_ACCESS_KEY_ID,
            TEST_SECRET_KEY,
            TEST_REGION,
            TEST_SERVICE,
            TEST_TIMESTAMP,
        )

        expected_auth_header = (
            "AWS4-HMAC-SHA256 "
            "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        )

        assert authorization_header == expected_auth_header

    def test_sign_request_v4_put_object(self):
        """
        Test complete signing process for PUT Object request.
        """
        method = "PUT"
        uri = "/test$file.text"
        query_string = ""
        headers = {
            "date": "Fri, 24 May 2013 00:00:00 GMT",
            "host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            "x-amz-date": "20130524T000000Z",
            "x-amz-storage-class": "REDUCED_REDUNDANCY",
        }
        payload_hash = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"

        authorization_header = sign_request_v4(
            method,
            uri,
            query_string,
            headers,
            payload_hash,
            TEST_ACCESS_KEY_ID,
            TEST_SECRET_KEY,
            TEST_REGION,
            TEST_SERVICE,
            TEST_TIMESTAMP,
        )

        expected_auth_header = (
            "AWS4-HMAC-SHA256 "
            "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
            "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, "
            "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        )

        assert authorization_header == expected_auth_header
