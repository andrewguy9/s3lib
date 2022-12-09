import pytest
import s3lib.ui
import os.path
import uuid
from hashlib import md5

@pytest.fixture
def testbucket():
    return 's3libtestbucket'

@pytest.fixture
def testkey():
    return str(uuid.uuid1())

@pytest.fixture
def testkey2():
    return str(uuid.uuid1())

@pytest.fixture
def testvalue():
    return str(uuid.uuid1()).encode('utf-8')

@pytest.fixture
def testcreds(tmp_path):
    cred_path = os.path.join(str(tmp_path), "creds")
    access = b'AKIAIOSFODNN7EXAMPLE'
    secret = b'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    creds = access + b'\n' + secret + b'\n'
    with open(cred_path, "wb") as fd: fd.write(creds)
    return cred_path

def test_s3ls_list_buckets(capsys, testbucket):
    captured = capsys.readouterr()
    s3lib.ui.ls_main([])
    captured = capsys.readouterr()
    assert testbucket in captured.out.split("\n")
    assert captured.err == ""

@pytest.mark.skip("Called indirectly")
def test_s3ls_list_bucket(capsys, testbucket, testkey=None, testetag=None, testsize=None):
    captured = capsys.readouterr()
    fields = []
    if testkey:
        fields.append('Key')
    if testetag:
        fields.append('ETag')
    if testsize:
        fields.append('Size')
    if fields == ['Key']:
        s3lib.ui.ls_main([testbucket])
    else:
        s3lib.ui.ls_main([testbucket, '--fields'] + fields)
    captured = capsys.readouterr()
    if testkey:
        assert testkey in captured.out
    if testetag:
        assert testetag in captured.out
    if testsize:
        assert str(testsize) in captured.out

    assert captured.err == ""

@pytest.mark.skip("Called indirectly")
def test_s3get(capsys, testbucket, testkey, testvalue):
    captured = capsys.readouterr()
    s3lib.ui.get_main([testbucket, testkey])
    captured = capsys.readouterr()
    assert captured.out.encode('utf-8') == testvalue
    assert captured.err == ""
# TODO test multiple KVs

def test_s3rm(capsys, testbucket, testkey):
    captured = capsys.readouterr()
    s3lib.ui.rm_main([testbucket, testkey])
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""

def test_s3put_file(tmp_path, capsys, testbucket, testkey, testkey2, testvalue):
    path = os.path.join(str(tmp_path), "test_file")
    with open(path, 'wb') as fd: fd.write(testvalue)
    etag = md5(testvalue).hexdigest()
    size = len(testvalue)
    captured = capsys.readouterr()
    s3lib.ui.put_main([testbucket, testkey, path])
    captured = capsys.readouterr()
    assert 'HTTP Code:  200' in captured.out
    assert captured.err == ""
    test_s3ls_list_bucket(capsys, testbucket)
    test_s3ls_list_bucket(capsys, testbucket, testkey, etag, size)
    test_s3get(capsys, testbucket, testkey, testvalue)
    test_s3head(capsys, testbucket, testkey, testvalue)
    test_s3cp(capsys, testbucket, testkey, testkey2)
    test_s3rm(capsys, testbucket, testkey)

def test_s3put_symlink(tmp_path, capsys, testbucket, testkey, testkey2, testvalue):
    path = os.path.join(str(tmp_path), "test_file")
    with open(path, 'wb') as fd: fd.write(testvalue)
    link_path = os.path.join(str(tmp_path), "test_link")
    os.symlink(path, link_path)
    captured = capsys.readouterr()
    s3lib.ui.put_main([testbucket, testkey, link_path])
    captured = capsys.readouterr()
    assert 'HTTP Code:  200' in captured.out
    assert captured.err == ""
    test_s3ls_list_bucket(capsys, testbucket, testkey)
    test_s3get(capsys, testbucket, testkey, testvalue)
    test_s3head(capsys, testbucket, testkey, testvalue)
    test_s3cp(capsys, testbucket, testkey, testkey2)
    test_s3rm(capsys, testbucket, testkey)

def test_s3put_dir(tmp_path, capsys, testbucket, testkey, testkey2, testvalue):
    path = os.path.join(str(tmp_path), "test_dir")
    os.mkdir(path)
    captured = capsys.readouterr()
    with pytest.raises(IOError):
        s3lib.ui.put_main([testbucket, testkey, path])
    captured = capsys.readouterr()

def test_s3put_pipe(tmp_path, capsys, testbucket, testkey, testkey2, testvalue, monkeypatch):
    path = os.path.join(str(tmp_path), "test_file")
    (r,w) = os.pipe()
    with os.fdopen(w, 'wb') as w:
        w.write(testvalue)
        w.flush()
    with os.fdopen(r, 'r') as r:
        monkeypatch.setattr('sys.stdin', r)
        s3lib.ui.put_main([testbucket, testkey])
    captured = capsys.readouterr()
    assert 'HTTP Code:  200' in captured.out
    assert captured.err == ""
    test_s3ls_list_bucket(capsys, testbucket, testkey)
    test_s3get(capsys, testbucket, testkey, testvalue)
    test_s3head(capsys, testbucket, testkey, testvalue)
    test_s3cp(capsys, testbucket, testkey, testkey2)
    test_s3rm(capsys, testbucket, testkey)

@pytest.mark.skip("Called indirectly")
def test_s3cp(capsys, testbucket, testkey, testkey2):
    captured = capsys.readouterr()
    s3lib.ui.cp_main([testbucket, testkey, testbucket, testkey2])
    captured = capsys.readouterr()
    assert 'HTTP Code:  200' in captured.out
    assert captured.err == ""

@pytest.mark.skip("Called indirectly")
def test_s3head(capsys, testbucket, testkey, testvalue):
    captured = capsys.readouterr()
    s3lib.ui.head_main([testbucket, testkey])
    captured = capsys.readouterr()
    etag = md5(testvalue).hexdigest()
    length = len(testvalue)
    assert 'content-length: ' + str(length)  in captured.out.lower()
    assert ("etag: " + '"' + etag + '"') in captured.out.lower()
    assert captured.err == ""

@pytest.mark.skip("Need to migrate to sign version 4 for examples to work")
def test_s3sign(capsys, tmp_path, testcreds):
    policy = """\
{ "expiration": "2015-12-30T12:00:00.000Z",\r
  "conditions": [\r
    {"bucket": "sigv4examplebucket"},\r
    ["starts-with", "$key", "user/user1/"],\r
    {"acl": "public-read"},\r
    {"success_action_redirect": "http://sigv4examplebucket.s3.amazonaws.com/successful_upload.html"},\r
    ["starts-with", "$Content-Type", "image/"],\r
    {"x-amz-meta-uuid": "14365123651274"},\r
    {"x-amz-server-side-encryption": "AES256"},\r
    ["starts-with", "$x-amz-meta-tag", ""],\r
\r
    {"x-amz-credential": "AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request"},\r
    {"x-amz-algorithm": "AWS4-HMAC-SHA256"},\r
    {"x-amz-date": "20151229T000000Z" }\r
  ]\r
}"""
    expected_signature = "8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e"
    expected_policy = "eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLA0KICAiY29uZGl0aW9ucyI6IFsNCiAgICB7ImJ1Y2tldCI6ICJzaWd2NGV4YW1wbGVidWNrZXQifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS8iXSwNCiAgICB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LA0KICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL3NpZ3Y0ZXhhbXBsZWJ1Y2tldC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRDb250ZW50LVR5cGUiLCAiaW1hZ2UvIl0sDQogICAgeyJ4LWFtei1tZXRhLXV1aWQiOiAiMTQzNjUxMjM2NTEyNzQifSwNCiAgICB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sDQogICAgWyJzdGFydHMtd2l0aCIsICIkeC1hbXotbWV0YS10YWciLCAiIl0sDQoNCiAgICB7IngtYW16LWNyZWRlbnRpYWwiOiAiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAxNTEyMjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LA0KICAgIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwNCiAgICB7IngtYW16LWRhdGUiOiAiMjAxNTEyMjlUMDAwMDAwWiIgfQ0KICBdDQp9"
    policy_path = os.path.join(str(tmp_path), "test_policy")
    with open(policy_path, "w") as fd: fd.write(policy)
    captured = capsys.readouterr()
    s3lib.ui.sign_main(['--creds', testcreds, policy_path])
    captured = capsys.readouterr()
    (b64policy, signature, _) = captured.out.split('\n')
    assert expected_policy == b64policy
    assert expected_signature == signature
    assert captured.err == ""
