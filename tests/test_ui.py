import pytest
import s3lib.ui
import os
import os.path
import uuid
from hashlib import md5


@pytest.fixture(params=[
    ("us-east-1", "s3libtestbucket"),
    ("us-west-1", "s3libtestbucket2"),
])
def testbucket_region(request):
    """Parametrized fixture providing (region, bucket) tuples for multi-region testing."""
    return request.param


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
    with open(cred_path, "wb") as fd:
        fd.write(creds)
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


def test_s3put_file(tmp_path, capsys, testbucket_region, testkey, testkey2, testvalue, monkeypatch):
    region, testbucket = testbucket_region
    # Set AWS_DEFAULT_REGION so Connection uses the correct region
    monkeypatch.setenv('AWS_DEFAULT_REGION', region)

    path = os.path.join(str(tmp_path), "test_file")
    with open(path, 'wb') as fd:
        fd.write(testvalue)
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
    with open(path, 'wb') as fd:
        fd.write(testvalue)
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
    with pytest.raises(IOError):
        s3lib.ui.put_main([testbucket, testkey, path])


def test_s3put_pipe(tmp_path, capsys, testbucket, testkey, testkey2, testvalue, monkeypatch):
    (r_fid, w_fid) = os.pipe()
    with os.fdopen(w_fid, 'wb') as w:
        w.write(testvalue)
        w.flush()
    with os.fdopen(r_fid, 'r') as r:
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
    assert 'content-length: ' + str(length) in captured.out.lower()
    assert ("etag: " + '"' + etag + '"') in captured.out.lower()
    assert captured.err == ""


@pytest.mark.skip("Need to migrate to sign version 4 for examples to work")
def test_s3sign(capsys, tmp_path, testcreds):
    import base64
    fixtures = os.path.join(os.path.dirname(__file__), "fixtures")
    policy_path = os.path.join(fixtures, "s3sign_policy.json")
    policy = open(policy_path).read()
    expected_signature = "8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e"
    expected_policy = base64.b64encode(policy.encode()).decode()
    captured = capsys.readouterr()
    s3lib.ui.sign_main(['--creds', testcreds, policy_path])
    captured = capsys.readouterr()
    (b64policy, signature, _) = captured.out.split('\n')
    assert expected_policy == b64policy
    assert expected_signature == signature
    assert captured.err == ""
