# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import pytest
import time

import unidist
from .utils import assert_equal, task, TestActor

unidist.init()


# FIXME: If we run all tests (i.e. pytest unidist/test), the tests are hanging.
# However, if we remove third argument, the tests will be passed. This is true for MultiProcessing backend and
# requires an investigation.
@pytest.mark.parametrize(
    "object_ref",
    [unidist.put(1), task.remote(3), TestActor.remote().task.remote(1)],
    ids=["put", "task", "actor"],
)
def test_is_object_ref(object_ref):
    assert_equal(unidist.is_object_ref(object_ref), True)


def test_wait():
    @unidist.remote
    def foo():
        time.sleep(3)
        return 1

    object_refs = [task.remote(5), foo.remote()]
    ready, not_ready = unidist.wait(object_refs, num_returns=1)
    assert_equal(len(ready), 1)
    assert_equal(len(not_ready), 1)
    ready, not_ready = unidist.wait(object_refs, num_returns=2)
    assert_equal(len(ready), 2)
    assert_equal(len(not_ready), 0)


def test_get_ip():
    import socket

    assert_equal(unidist.get_ip(), socket.gethostbyname(socket.gethostname()))


def test_num_cpus():
    import multiprocessing as mp

    assert_equal(unidist.num_cpus(), mp.cpu_count())
