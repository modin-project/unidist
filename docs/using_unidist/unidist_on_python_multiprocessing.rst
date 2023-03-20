..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist on Python Multiprocessing
'''''''''''''''''''''''''''''''''

This section describes the use of unidist with the Python Multiprocessing execution backend.

There are two ways to choose the execution backend to run on.
First, by setting the ``UNIDIST_BACKEND`` environment variable:

.. code-block:: bash

    # unidist will use Python Multiprocessing backend
    $ export UNIDIST_BACKEND=multiprocessing

.. code-block:: python

    import os

    # unidist will use Python Multiprocessing backend
    os.environ["UNIDIST_BACKEND"] = "multiprocessing"

Second, by setting the configuration value associated with the environment variable:

.. code-block:: python

    from unidist.config import Backend

    Backend.put("multiprocessing")  # unidist will use Python Multiprocessing backend

For more information on the environment variables and associated configs specific to the Python Multiprocessing backend
see :doc:`config API </flow/unidist/config>` section.
