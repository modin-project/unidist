# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Utilities for the backends."""

from unidist import is_object_ref


def unwrap_object_refs(obj_refs):
    """Find all ``unidist.core.base.ObjectRef`` instances and unwrap underlying objects.

    Parameters
    ----------
    obj_refs : iterable or ObjectRef
        Iterable objects to transform recursively.

    Returns
    -------
    iterable or underlying object of ObjectRef
    """
    if type(obj_refs) in (list, tuple, dict):
        container = type(obj_refs)()
        for value in obj_refs:
            unwrapped_value = unwrap_object_refs(
                obj_refs[value] if isinstance(obj_refs, dict) else value
            )
            if isinstance(container, list):
                container += [unwrapped_value]
            elif isinstance(container, tuple):
                container += (unwrapped_value,)
            elif isinstance(container, dict):
                container.update({value: unwrapped_value})
        return container
    else:
        return obj_refs._ref if is_object_ref(obj_refs) else obj_refs
