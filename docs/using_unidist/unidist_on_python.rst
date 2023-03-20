..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist on Python
'''''''''''''''''

This section describes the use of unidist with the sequential Python execution backend,
which is for debugging.

There are two ways to choose the execution backend to run on.
First, by setting the ``UNIDIST_BACKEND`` environment variable:

.. code-block:: bash

    # unidist will use sequential Python backend
    $ export UNIDIST_BACKEND=python

.. code-block:: python

    import os

    # unidist will use sequential Python backend
    os.environ["UNIDIST_BACKEND"] = "python"

Second, by setting the configuration value associated with the environment variable:

.. code-block:: python

    from unidist.config import Backend

    Backend.put("python")  # unidist will use sequential Python backend

For more information on the environment variables and associated configs specific to the Python backend
see :doc:`config API </flow/unidist/config>` section.
