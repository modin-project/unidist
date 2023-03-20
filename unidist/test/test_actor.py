# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import sys
import pytest
import gc

import unidist
from unidist.config import Backend, CpuCount
from unidist.core.base.common import BackendName
from .utils import assert_equal, TestActor

unidist.init()


@pytest.fixture(autouse=True)
def call_gc_collect():
    """
    Collect all references from the previous test in order for MPI backend to work correctly.
    """
    yield
    # This is only needed for the MPI backend
    if Backend.get() == BackendName.MPI:
        gc.collect()


@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.PYMP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
@pytest.mark.parametrize("is_default_constructor", [True, False])
def test_actor_constructor(is_default_constructor):
    actor = TestActor.remote() if is_default_constructor else TestActor.remote(5)
    assert_equal(actor.get_accumulator.remote(), 0 if is_default_constructor else 5)


@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.PYMP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
def test_chaining():
    object_ref = unidist.put(7)
    actor = TestActor.remote()
    assert_equal(actor.task.remote(object_ref), 8)


@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.PYMP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
def test_num_returns():
    actor = TestActor.remote(7)
    object_ref0, object_ref1 = actor.multiple_returns.options(num_returns=2).remote(3)
    assert_equal(object_ref0, 3)
    assert_equal(object_ref1, 7)


@pytest.mark.skipif(
    Backend.get() == BackendName.PYMP,
    reason="`pymp` backend incorrectly frees grabbed actors. Details are in https://github.com/modin-project/unidist/issues/65.",
)
@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.PYMP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
@pytest.mark.parametrize("is_use_options", [True, False])
def test_address_space(is_use_options):
    actor0 = TestActor.options().remote(0) if is_use_options else TestActor.remote(0)
    actor1 = TestActor.options().remote(1) if is_use_options else TestActor.remote(1)

    object_ref0 = actor0.get_accumulator.remote()
    object_ref1 = actor1.get_accumulator.remote()

    assert_equal(object_ref0, 0)
    assert_equal(object_ref1, 1)


@pytest.mark.skipif(
    Backend.get() == BackendName.PYMP,
    reason="Proper serialization/deserialization is not implemented yet for pymp",
)
def test_global_capture():
    actor = TestActor.remote(0)

    @unidist.remote
    def foo():
        object_ref = actor.get_accumulator.remote()
        return unidist.get(object_ref)

    object_ref = foo.remote()

    assert_equal(object_ref, 0)


@pytest.mark.skipif(
    Backend.get() == BackendName.PYMP,
    reason="Proper serialization/deserialization is not implemented yet for pymp",
)
def test_direct_capture():
    actor = TestActor.remote(0)

    @unidist.remote
    def foo(actor):
        object_ref = actor.get_accumulator.remote()
        return unidist.get(object_ref)

    object_ref = foo.remote(actor)

    assert_equal(object_ref, 0)


@pytest.mark.skipif(
    Backend.get() == BackendName.PYMP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
def test_return_none():
    actor = TestActor.remote()
    assert_equal(actor.task_return_none.remote(), None)


@pytest.mark.skipif(
    Backend.get() == BackendName.PYMP,
    reason="Run of a remote task inside of an actor method is not implemented yet for pymp",
)
@pytest.mark.skipif(
    Backend.get() == BackendName.DASK,
    reason="Dask hungs when the number of tasks exceeds the number of cores",
)
def test_actor_scheduling():
    actor = TestActor.remote()

    @unidist.remote
    def f():
        return unidist.get(actor.get_accumulator.remote())

    # Use a larger number of cores deliberately to check the assignment
    for _ in range(CpuCount.get()):
        assert_equal(f.remote(), 0)
