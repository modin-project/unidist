# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import unidist


def materialize(object_refs):
    return (
        unidist.get(object_refs)
        if (
            all(unidist.is_object_ref(obj_ref) for obj_ref in object_refs)
            if isinstance(object_refs, list)
            else unidist.is_object_ref(object_refs)
        )
        else object_refs
    )


def assert_equal(object_refs, expected_result):
    actual_result = materialize(object_refs)
    assert (
        actual_result == expected_result
    ), f"Actual result is <{actual_result}>, but expected <{expected_result}>."


def catch_exception(object_refs, expected_exception):
    try:
        _ = materialize(object_refs)
    except Exception as e:
        if not isinstance(e, expected_exception):
            raise Exception(f"Got {type(e)}, but {expected_exception} was expected.")
    else:
        raise Exception(
            f"Exception wasn't raised, but {expected_exception} was expected."
        )


@unidist.remote
def task(x):
    return x * x


@unidist.remote
def task_return_none():
    return None


@unidist.remote(num_returns=2)
def task_multiple_returns_default(x):
    return x, x * x


@unidist.remote
def task_multiple_returns(x):
    return x, x * x


@unidist.remote
class TestActor:
    _accumulator = 0

    def __init__(self, init=0):
        self._accumulator = init

    def get_accumulator(self):
        return self._accumulator

    def _internal(self):
        self._accumulator += 1

    def task(self, x):
        self._accumulator += x
        self._internal()
        return self._accumulator

    def task_return_none(self):
        return None

    def multiple_returns(self, x):
        return x, self._accumulator


@unidist.remote
class TestAsyncActor:
    _accumulator = 0

    def __init__(self, init=0):
        self._accumulator = init

    async def get_accumulator(self):
        return self._accumulator

    def _internal(self):
        self._accumulator += 1

    async def task(self, x):
        self._accumulator += x
        self._internal()
        return self._accumulator

    async def task_return_none(self):
        return None

    async def multiple_returns(self, x):
        return x, self._accumulator
