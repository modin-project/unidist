# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

import pytest

import unidist
from .utils import assert_equal, TestActor

unidist.init()


@pytest.mark.parametrize("is_default_constructor", [True, False])
def test_actor_constructor(is_default_constructor):
    actor = TestActor.remote() if is_default_constructor else TestActor.remote(5)
    assert_equal(actor.get_accumulator.remote(), 0 if is_default_constructor else 5)


def test_chaining():
    object_ref = unidist.put(7)
    actor = TestActor.remote()
    assert_equal(actor.task.remote(object_ref), 8)


def test_num_returns():
    actor = TestActor.remote(7)
    object_ref0, object_ref1 = actor.multiple_returns.options(num_returns=2).remote(3)
    assert_equal(object_ref0, 3)
    assert_equal(object_ref1, 7)


@pytest.mark.parametrize("is_use_options", [True, False])
def test_address_space(is_use_options):
    actor0 = TestActor.options().remote(0) if is_use_options else TestActor.remote(0)
    actor1 = TestActor.options().remote(1) if is_use_options else TestActor.remote(1)

    object_ref0 = actor0.get_accumulator.remote()
    object_ref1 = actor1.get_accumulator.remote()

    assert_equal(object_ref0, 0)
    assert_equal(object_ref1, 1)
