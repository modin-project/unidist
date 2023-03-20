..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist on Dask
'''''''''''''''

This section describes the use of unidist with the Dask execution backend.

There are two ways to choose the execution backend to run on.
First, by setting the ``UNIDIST_BACKEND`` environment variable:

.. code-block:: bash

    # unidist will use Dask
    $ export UNIDIST_BACKEND=dask

.. code-block:: python

    import os

    # unidist will use Dask
    os.environ["UNIDIST_BACKEND"] = "dask"

Second, by setting the configuration value associated with the environment variable:

.. code-block:: python

    from unidist.config import Backend

    Backend.put("dask")  # unidist will use Dask

For more information on the environment variables and associated configs specific to the Dask backend
see :doc:`config API </flow/unidist/config>` section.


Unidist on Dask cluster
'''''''''''''''''''''''

Currently, in order to use unidist with Dask on a cluster, Dask cluster needs to be pre-initialized.
Please refer to its own documentation `Dask Guide <https://distributed.dask.org/en/latest/>`_
on how to set up a cluster.
