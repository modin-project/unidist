..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist on Ray
''''''''''''''

This section describes the use of unidist with the Ray execution backend.

There are two ways to choose the execution backend to run on.
First, by setting the ``UNIDIST_BACKEND`` environment variable:

.. code-block:: bash

    # unidist will use Ray
    $ export UNIDIST_BACKEND=ray

.. code-block:: python

    import os

    # unidist will use Ray
    os.environ["UNIDIST_BACKEND"] = "ray"

Second, by setting the configuration value associated with the environment variable:

.. code-block:: python

    from unidist.config import Backend

    Backend.put("ray")  # unidist will use Ray

For more information on the environment variables and associated configs specific to the Ray backend
see :doc:`config API </flow/unidist/config>` section.

Unidist on Ray cluster
''''''''''''''''''''''

Currently, in order to use unidist with Ray on a cluster, Ray cluster needs to be pre-initialized.
Please refer to its own documentation `Ray Guide <https://docs.ray.io/en/latest/index.html>`_
on how to set up a cluster.
