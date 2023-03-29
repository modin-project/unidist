# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import time
import pytest

import unidist
from unidist.config import Backend
from unidist.core.base.common import BackendName
from .utils import (
    assert_equal,
    catch_exception,
    task,
    task_return_none,
    task_multiple_returns_default,
    task_multiple_returns,
)

unidist.init()


def test_remote_get():
    assert_equal(task.remote(5), 25)


def test_chaining_value():
    object_ref = unidist.put(6)
    object_refs = [task.remote(object_ref) for _ in range(4)]
    assert_equal(object_refs, [36, 36, 36, 36])


def test_num_returns_decorator_options():
    object_ref0, object_ref1 = task_multiple_returns_default.remote(5)
    assert_equal(object_ref0, 5)
    assert_equal(object_ref1, 25)


def test_num_returns_options():
    object_ref0, object_ref1 = task_multiple_returns.options(num_returns=2).remote(5)
    assert_equal(object_ref0, 5)
    assert_equal(object_ref1, 25)


def test_put_lambda():
    object_ref0 = unidist.put(lambda x: x * x)

    @unidist.remote
    def foo(f):
        return f(3)

    object_ref1 = foo.remote(object_ref0)
    assert_equal(object_ref1, 9)


def test_delayed_object_ref():
    @unidist.remote
    def foo():
        time.sleep(3)
        return 7

    object_ref0 = foo.remote()
    object_ref1 = task.remote(object_ref0)
    assert_equal(object_ref1, 49)


def test_num_returns_zero():
    @unidist.remote
    def foo():
        pass

    assert_equal(foo.options(num_returns=0).remote(), None)


def test_exception():
    @unidist.remote
    def foo(x):
        return x / x

    catch_exception(foo.remote(0), ZeroDivisionError)


def test_return_none():
    assert_equal(task_return_none.remote(), None)


@pytest.mark.skipif(
    Backend.get() == BackendName.PYMP,
    reason="Run of a remote task inside of another one is not implemented yet for pymp",
)
def test_internal_remote():
    @unidist.remote
    def foo(x):
        
        o_r = task.remote(x)
        return unidist.get(o_r) + 10

    assert_equal(foo.remote(5), 35)


@pytest.mark.skipif(
    Backend.get() == BackendName.PYMP,
    reason="Serialization of `dict_keys` is not properly implemented yet for pymp",
)
def test_serialize_dict_keys():
    @unidist.remote
    def f(keys):
        return keys

    dict_obj = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5}

    assert_equal(f.remote(dict_obj.keys()), dict_obj.keys())


def test_serialize_dict_with_tuple_key():
    @unidist.remote
    def f(params):
        return params

    data = {
        "a": [0, 1, 2, 3],
        "b": {("a", "b", "c", "d"): "value"},
    }

    assert_equal(f.remote(data), data)
