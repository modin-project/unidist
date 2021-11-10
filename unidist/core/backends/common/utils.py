# Copyright (C) 2021 Modin authors
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
    if isinstance(obj_refs, (list, tuple, dict)):
        container = type(obj_refs)()
        for value in obj_refs:
            if isinstance(value, (list, tuple, dict)):
                unwrapped_value = unwrap_object_refs(
                    {value: obj_refs[value]} if isinstance(obj_refs, dict) else value
                )
                if isinstance(container, list):
                    container += [unwrapped_value]
                elif isinstance(container, tuple):
                    container += (unwrapped_value,)
                elif isinstance(container, dict):
                    container.update(unwrapped_value)
            else:
                if isinstance(container, list):
                    container += [value._ref] if is_object_ref(value) else [value]
                elif isinstance(container, tuple):
                    container += (value._ref,) if is_object_ref(value) else (value,)
                elif isinstance(container, dict):
                    container[value] = (
                        obj_refs[value]._ref
                        if is_object_ref(obj_refs[value])
                        else unwrap_object_refs(obj_refs[value])
                        if isinstance(obj_refs[value], (list, tuple, dict))
                        else obj_refs[value]
                    )
        return container
    else:
        return obj_refs._ref if is_object_ref(obj_refs) else obj_refs
