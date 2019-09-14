
def validate_signature(string, expected_string, expected_signature):
  assert(string == expected_string)
  secret = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  signature = sign(secret, string)
  assert(signature == expected_signature)

def test_sign_get():
  string = _get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:36:42 +0000", {}, "/johnsmith/photos/puppy.jpg")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg"
  expected_signature = "bWq2s1WEIj+Ydj0vQ697zp+IXMU="
  validate_signature(string, expected_string, expected_signature)

def test_sign_put():
  string = _get_string_to_sign("PUT", "", "image/jpeg", "Tue, 27 Mar 2007 21:15:45 +0000", {}, "/johnsmith/photos/puppy.jpg" )
  expected_string = "PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg"
  expected_signature = "MyyxeRY7whkBe+bq8fHCL/2kKUg="
  validate_signature(string, expected_string, expected_signature)

def test_sign_list():
  string = _get_string_to_sign("GET","", "", "Tue, 27 Mar 2007 19:42:41 +0000", {}, "/johnsmith/")
  expected_string = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/"
  expected_signature = "htDYFYduRNen8P9ZfE/s9SuKy0U="
  validate_signature(string, expected_string, expected_signature)

# TODO this test should be disabled for failing.
def test_sign_copy():
  string = _get_string_to_sign("PUT", "", "", "Wed, 20 Feb 2008 22:12:21 +0000", {"x-amz-copy-source":"/pacific/flotsam"}, "/atlantic/jetsam")
  expected_string = "PUT\n\n\nWed, 20 Feb 2008 22:12:21 +0000\nx-amz-copy-source:/pacific/flotsam\n/atlantic/jetsam"
  expected_signature = "ENoSbxYByFA0UGLZUqJN5EUnLDg="
  validate_signature(string, expected_string, expected_signature)


def test_take():
  assert(list(_take(3, [])) == [])
  assert(list(_take(3, [1,2])) == [1,2])
  assert(list(_take(3, [1,2, 3, 4])) == [1,2,3])
  i = iter(range(7))
  assert(list(_take(3, i)) == [0,1,2])
  assert(list(_take(3, i)) == [3,4,5])
  assert(list(_take(3, i)) == [6])

def test_batchify():
  assert(list(_batchify(3, [])) == [])
  assert(list(_batchify(3, [1])) == [[1]])
  assert(list(_batchify(3, [1,2,3])) == [[1,2,3]])
  assert(list(_batchify(3, [1,2,3,4])) == [[1,2,3],[4]])
  assert(list(_batchify(3, iter([1,2,3,4]))) == [[1,2,3],[4]])
