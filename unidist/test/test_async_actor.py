# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
import sys
import pytest
import gc

import unidist
from unidist.config import Backend, CpuCount
from unidist.core.base.common import BackendName
from .utils import assert_equal, TestAsyncActor

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
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="MP and PY backends do not support execution of coroutines.",
)
@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.MP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
@pytest.mark.parametrize("is_default_constructor", [True, False])
def test_actor_constructor(is_default_constructor):
    actor = (
        TestAsyncActor.remote() if is_default_constructor else TestAsyncActor.remote(5)
    )
    assert_equal(actor.get_accumulator.remote(), 0 if is_default_constructor else 5)


@pytest.mark.skipif(
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="MP and PY backends do not support execution of coroutines.",
)
@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.MP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
def test_chaining():
    object_ref = unidist.put(7)
    actor = TestAsyncActor.remote()
    assert_equal(actor.task.remote(object_ref), 8)


@pytest.mark.skipif(
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="MP and PY backends do not support execution of coroutines.",
)
@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.MP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
def test_num_returns():
    actor = TestAsyncActor.remote(7)
    object_ref0, object_ref1 = actor.multiple_returns.options(num_returns=2).remote(3)
    assert_equal(object_ref0, 3)
    assert_equal(object_ref1, 7)


@pytest.mark.skipif(
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="MP and PY backends do not support execution of coroutines.",
)
@pytest.mark.skipif(
    Backend.get() == BackendName.MP,
    reason="`multiprocessing` backend incorrectly frees grabbed actors. Details are in https://github.com/modin-project/unidist/issues/65.",
)
@pytest.mark.skipif(
    sys.platform == "win32" and Backend.get() == BackendName.MP,
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
@pytest.mark.parametrize("is_use_options", [True, False])
def test_address_space(is_use_options):
    actor0 = (
        TestAsyncActor.options().remote(0)
        if is_use_options
        else TestAsyncActor.remote(0)
    )
    actor1 = (
        TestAsyncActor.options().remote(1)
        if is_use_options
        else TestAsyncActor.remote(1)
    )

    object_ref0 = actor0.get_accumulator.remote()
    object_ref1 = actor1.get_accumulator.remote()

    assert_equal(object_ref0, 0)
    assert_equal(object_ref1, 1)


@pytest.mark.skipif(
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="Proper serialization/deserialization is not implemented yet for multiprocessing and python",
)
def test_global_capture():
    actor = TestAsyncActor.remote(0)

    @unidist.remote
    def foo():
        object_ref = actor.get_accumulator.remote()
        return unidist.get(object_ref)

    object_ref = foo.remote()

    assert_equal(object_ref, 0)


@pytest.mark.skipif(
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="Proper serialization/deserialization is not implemented yet for multiprocessing and python",
)
def test_direct_capture():
    actor = TestAsyncActor.remote(0)

    @unidist.remote
    def foo(actor):
        object_ref = actor.get_accumulator.remote()
        return unidist.get(object_ref)

    object_ref = foo.remote(actor)

    assert_equal(object_ref, 0)


@pytest.mark.skipif(
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="Details are in https://github.com/modin-project/unidist/issues/70.",
)
def test_return_none():
    actor = TestAsyncActor.remote()
    assert_equal(actor.task_return_none.remote(), None)


@pytest.mark.skipif(
    Backend.get() in (BackendName.MP, BackendName.PY),
    reason="MP and PY backends do not support execution of coroutines.",
)
def test_pending_get():
    @unidist.remote
    class SlowActor:
        async def slow_execute(self):
            await asyncio.sleep(5)
            return 1

    slow_actor = SlowActor.remote()

    @unidist.remote
    def g():
        return unidist.get(slow_actor.slow_execute.remote()) + 1

    assert_equal(g.remote(), 2)


@pytest.mark.skipif(
    Backend.get() == BackendName.DASK,
    reason="Unexpected exception `There is no current event loop in thread` is raised",
)
@pytest.mark.skipif(
    Backend.get() == BackendName.MP,
    reason="Run of a remote task inside of an async actor method is not implemented yet for multiprocessing",
)
def test_signal_actor():
    @unidist.remote
    class SignalActor:
        def __init__(self, event_count: int):
            self.events = [asyncio.Event() for _ in range(event_count)]

        def send(self, event_idx: int):
            self.events[event_idx].set()

        async def wait(self, event_idx: int):
            await self.events[event_idx].wait()

    signals = SignalActor.remote(CpuCount.get() + 1)

    unidist.get(signals.send.remote(0))

    @unidist.remote
    def func(idx):
        unidist.get(signals.wait.remote(idx))
        unidist.get(signals.send.remote(idx + 1))
        return idx

    object_refs = [func.remote(idx) for idx in range(CpuCount.get())]

    assert_equal(object_refs, list(range(CpuCount.get())))
