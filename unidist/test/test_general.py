# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import sys
import pytest
import time

import unidist
from unidist.config import Backend, CpuCount
from unidist.core.base.common import BackendName
from .utils import assert_equal, task, TestActor

unidist.init()


@pytest.mark.skipif(
    Backend.get() == BackendName.MP,
    reason="Hangs on `multiprocessing` backend. Details are in https://github.com/modin-project/unidist/issues/64.",
)
@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.MP,
    reason="",
)
@pytest.mark.parametrize(
    "object_ref",
    [unidist.put(1), task.remote(3), TestActor.remote().task.remote(1)],
    ids=["put", "task", "actor"],
)
def test_is_object_ref(object_ref):
    assert_equal(unidist.is_object_ref(object_ref), True)


@pytest.mark.skipif(
    Backend.get() == BackendName.MP,
    reason="Hangs on `multiprocessing` backend. Details are in https://github.com/modin-project/unidist/issues/64.",
)
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
    if Backend.get() == BackendName.PY:
        assert_equal(unidist.num_cpus(), 1)
    else:
        assert_equal(unidist.num_cpus(), CpuCount.get())
