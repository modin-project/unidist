# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import pytest
import time

import unidist
from unidist.config import Backend, CpuCount
from unidist.core.base.common import BackendName
from .utils import assert_equal, task, TestActor

unidist.init()


@pytest.mark.parametrize(
    "source",
    ["put", "task", "actor"],
)
def test_is_object_ref(source):
    if source == "put":
        object_ref = unidist.put(1)
    elif source == "task":
        object_ref = task.remote(3)
    else:
        object_ref = TestActor.remote().task.remote(1)
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

    try:
        assert_equal(unidist.get_ip(), socket.gethostbyname(socket.gethostname()))
    # Sometimes Ray returns localhost IP address in GH actions so we check this too
    except AssertionError:
        assert_equal(unidist.get_ip(), "127.0.0.1")


def test_num_cpus():
    if Backend.get() == BackendName.PY:
        assert_equal(unidist.num_cpus(), 1)
    else:
        assert_equal(unidist.num_cpus(), CpuCount.get())


def test_cluster_resources():
    assert_equal(
        unidist.cluster_resources(), {unidist.get_ip(): {"CPU": unidist.num_cpus()}}
    )
