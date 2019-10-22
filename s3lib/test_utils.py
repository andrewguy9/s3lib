from s3lib.utils import *

def test_take():
  assert(list(take(3, [])) == [])
  assert(list(take(3, [1,2])) == [1,2])
  assert(list(take(3, [1,2, 3, 4])) == [1,2,3])
  i = iter(list(range(7)))
  assert(list(take(3, i)) == [0,1,2])
  assert(list(take(3, i)) == [3,4,5])
  assert(list(take(3, i)) == [6])

def test_batchify():
  assert(list(batchify(3, [])) == [])
  assert(list(batchify(3, [1])) == [[1]])
  assert(list(batchify(3, [1,2,3])) == [[1,2,3]])
  assert(list(batchify(3, [1,2,3,4])) == [[1,2,3],[4]])
  assert(list(batchify(3, iter([1,2,3,4]))) == [[1,2,3],[4]])

def test_split_args():
  assert split_args({"delete":None}) == {"delete": None}
  assert split_args({"delete":None, 'a':'b'}) == {"delete": None}
