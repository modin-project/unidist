..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist on PySeq
''''''''''''''''

This section describes the use of unidist with the Python Sequential execution backend,
which is for debugging.

There are two ways to choose the execution backend to run on.
First, by setting the ``UNIDIST_BACKEND`` environment variable:

.. code-block:: bash

    # unidist will use Python Sequential backend
    $ export UNIDIST_BACKEND=pyseq

.. code-block:: python

    import os

    # unidist will use Python Sequential backend
    os.environ["UNIDIST_BACKEND"] = "pyseq"

Second, by setting the configuration value associated with the environment variable:

.. code-block:: python

    from unidist.config import Backend

    Backend.put("pyseq")  # unidist will use Python Sequential backend

For more information on the environment variables and associated configs specific to the Python Sequential backend
see :doc:`config API </flow/unidist/config>` section.
