# Copyright (C) 2021-2022 Modin authors
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
        decode=lambda value: value.strip().lower(),
        normalize=lambda value: value.strip().lower(),
        verify=lambda value: isinstance(value, str),
        help="a case-insensitive string",
    ),
    ExactStr: TypeDescriptor(
        decode=lambda value: value,
        normalize=lambda value: value,
        verify=lambda value: isinstance(value, str),
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


class ValueSource:
    """Class that describes the method of getting the value for a parameter."""

    # got from default, i.e. neither user nor configuration source had the value
    DEFAULT = 0
    # set by user
    SET_BY_USER = 1
    # got from parameter configuration source, like environment variable
    GOT_FROM_CFG_SOURCE = 2


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
    _value_source : int
        Source of the ``Parameter`` value, should be set by
        ``ValueSource``.
    """

    choices: typing.Sequence[str] = None
    type = str
    default = None
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

    def __init_subclass__(cls, type, **kw):
        """
        Initialize subclass.

        Parameters
        ----------
        type : Any
            Type of the config.
        **kw : dict
            Optional arguments for config initialization.
        """
        assert type in _TYPE_PARAMS, f"Unsupported variable type: {type}"
        cls.type = type
        cls._value = _UNSET
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
    def get_value_source(cls):
        """
        Get value source of the config.

        Returns
        -------
        int
        """
        if cls._value_source is None:
            # dummy call to .get() to initialize the value
            cls.get()
        return cls._value_source

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
                cls._value_source = ValueSource.DEFAULT
            else:
                if not _TYPE_PARAMS[cls.type].verify(raw):
                    raise ValueError(f"Unsupported raw value: {raw}")
                cls._value = _TYPE_PARAMS[cls.type].decode(raw)
                cls._value_source = ValueSource.GOT_FROM_CFG_SOURCE

        if cls.choices is not None and cls._value not in cls.choices:
            raise ValueError(
                f"Unsupported value '{cls._value}'. Supported set of values is {cls.choices}."
            )
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
        if not _TYPE_PARAMS[cls.type].verify(value):
            raise ValueError(f"Unsupported value: {value}")
        cls._value = _TYPE_PARAMS[cls.type].normalize(value)
        cls._value_source = ValueSource.SET_BY_USER


class EnvironmentVariable(Parameter, type=str):
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
