# Copyright (C) 2021 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""Base entities for all configs."""

import os
import typing


class TypeDescriptor(typing.NamedTuple):
    """
    Class for config data manipulating of exact type.

    Parameters
    ----------
    decode : callable
        Callable to decode config value from the raw data.
    normalize : callable
        Callable to bring different config value variations to
        the single form.
    verify : callable
        Callable to check that config value satisfies given config
        type requirements.
    help : str
        Class description string.
    """

    decode: typing.Callable[[str], object]
    normalize: typing.Callable[[object], object]
    verify: typing.Callable[[object], bool]
    help: str


class ExactStr(str):
    """Class to be used in type params where no transformations are needed."""


_TYPE_PARAMS = {
    str: TypeDescriptor(
        decode=lambda value: value.strip().title(),
        normalize=lambda value: value.strip().title(),
        verify=lambda value: True,
        help="a case-insensitive string",
    ),
    ExactStr: TypeDescriptor(
        decode=lambda value: value,
        normalize=lambda value: value,
        verify=lambda value: True,
        help="a string",
    ),
    bool: TypeDescriptor(
        decode=lambda value: value.strip().lower() in {"true", "yes", "1"},
        normalize=bool,
        verify=lambda value: isinstance(value, bool)
        or (
            isinstance(value, str)
            and value.strip().lower() in {"true", "yes", "1", "false", "no", "0"}
        ),
        help="a boolean flag (any of 'true', 'yes' or '1' in case insensitive manner is considered positive)",
    ),
    int: TypeDescriptor(
        decode=lambda value: int(value.strip()),
        normalize=int,
        verify=lambda value: isinstance(value, int)
        or (isinstance(value, str) and value.strip().isdigit()),
        help="an integer value",
    ),
}

# Special marker to distinguish unset value from ``None`` value
# as someone may want to use ``None`` as a real value for a parameter
_UNSET = object()


class Parameter(object):
    """
    Base class describing interface for configuration entities.

    Attributes
    ----------
    choices : sequence of str
        Array with possible options of ``Parameter`` values.
    type : str
        String that denotes ``Parameter`` type.
    default : Any
        ``Parameter`` default value.
    is_abstract : bool, default: True
        Whether or not ``Parameter`` is abstract.
    _value_source : int
        Source of the ``Parameter`` value, should be set by
        ``ValueSource``.
    """

    choices: typing.Sequence[str] = None
    type = str
    default = None
    is_abstract = True
    _value_source = None

    @classmethod
    def _get_raw_from_config(cls) -> str:
        """
        Read the value from config storage.

        Returns
        -------
        str
            Config raw value.

        Raises
        ------
        KeyError
            If value is absent.

        Notes
        -----
        Config storage can be config file or environment variable or whatever.
        Method should be implemented in the child class.
        """
        raise NotImplementedError()

    def __init_subclass__(cls, type, abstract=False, **kw):
        """
        Initialize subclass.

        Parameters
        ----------
        type : Any
            Type of the config.
        abstract : bool, default: False
            Whether config is abstract.
        **kw : dict
            Optional arguments for config initialization.
        """
        assert type in _TYPE_PARAMS, f"Unsupported variable type: {type}"
        cls.type = type
        cls.is_abstract = abstract
        cls._value = _UNSET
        cls._subs = []
        super().__init_subclass__(**kw)

    @classmethod
    def _get_default(cls):
        """
        Get default value of the config.

        Returns
        -------
        Any
        """
        return cls.default

    @classmethod
    def get(cls):
        """
        Get config value.

        Returns
        -------
        Any
            Decoded and verified config value.
        """
        if cls._value is _UNSET:
            # get the value from env
            try:
                raw = cls._get_raw_from_config()
            except KeyError:
                cls._value = cls._get_default()
            else:
                if not _TYPE_PARAMS[cls.type].verify(raw):
                    raise ValueError(f"Unsupported raw value: {raw}")
                cls._value = _TYPE_PARAMS[cls.type].decode(raw)
        return cls._value

    @classmethod
    def put(cls, value):
        """
        Set config value.

        Parameters
        ----------
        value : Any
            Config value to set.
        """
        cls._put_config_value(value)

    @classmethod
    def _put_config_value(cls, value):
        """
        Set config value.

        Parameters
        ----------
        value : Any
            Config value to set.

        Returns
        -------
        Any
            Replaced (old) config value.
        """
        if not _TYPE_PARAMS[cls.type].verify(value):
            raise ValueError(f"Unsupported value: {value}")
        value = _TYPE_PARAMS[cls.type].normalize(value)
        oldvalue, cls._value = cls.get(), value
        return oldvalue


class EnvironmentVariable(Parameter, type=str, abstract=True):
    """Base class for environment variables-based configuration."""

    varname: str = None

    @classmethod
    def _get_raw_from_config(cls) -> str:
        """
        Read the value from environment variable.

        Returns
        -------
        str
            Config raw value.

        Raises
        ------
        KeyError
            If value is absent.
        """
        return os.environ[cls.varname]
