from s3lib.utils import batchify, rate_limited, split_args, take
from unittest.mock import MagicMock
import pytest


def test_take():
    assert list(take(3, [])) == []
    assert list(take(3, [1, 2])) == [1, 2]
    assert list(take(3, [1, 2, 3, 4])) == [1, 2, 3]
    i = iter(list(range(7)))
    assert list(take(3, i)) == [0, 1, 2]
    assert list(take(3, i)) == [3, 4, 5]
    assert list(take(3, i)) == [6]


def test_batchify():
    assert list(batchify(3, [])) == []
    assert list(batchify(3, [1])) == [[1]]
    assert list(batchify(3, [1, 2, 3])) == [[1, 2, 3]]
    assert list(batchify(3, [1, 2, 3, 4])) == [[1, 2, 3], [4]]
    assert list(batchify(3, iter([1, 2, 3, 4]))) == [[1, 2, 3], [4]]


def test_split_args():
    assert split_args({"delete": None}) == {"delete": None}
    assert split_args({"delete": None, 'a': 'b'}) == {"delete": None}


@pytest.mark.parametrize(
    "rate,calls,latency,expected_clock, expected_sleep",
    [
        # Work slower than limit, no sleeps.
        (2,  10, 1, 10, 0),
        # Calls are balanced with time, so no sleeping.
        (1,  10, 1, 10, 0),
        # 1/2 the time we are early and sleep for 1 sec.
        (1/2.0, 10, 1, 10+5, 5*1),
    ],)
def test_rate_limited(rate, calls, latency,
                      expected_clock, expected_sleep):
    sleeper = MagicMock(side_effect=lambda t: print("slept", t))
    doer = MagicMock(side_effect=lambda: print("doing"))

    def time_slept():
        return sum(map(lambda call: call[0][0], sleeper.call_args_list))

    def current_time():
        now = doer.call_count * latency + time_slept()
        print("now:", now)
        return now

    clock = MagicMock(side_effect=current_time)
    limited = rate_limited(rate, clock, sleeper)(doer)
    for _ in range(calls):
        print("about to call", current_time(),
              "last time", limited.last_time_called)
        limited()

    print(dir(sleeper))
    print("slept", sleeper.call_count)
    print("clock", clock.call_count)
    print("doer", doer.call_count)
    print("clock", sleeper.call_args_list)
    print("time slept", time_slept())
    assert clock() == expected_clock
    assert time_slept() == expected_sleep
