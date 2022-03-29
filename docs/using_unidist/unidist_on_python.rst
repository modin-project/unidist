..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

unidist on Python
'''''''''''''''''

This section describes the use of unidist with the sequential Python execution backend,
which is for debugging.

There are two ways to choose the execution backend to run on.
First, the recommended way is to use the argument of unidist CLI:

.. code-block:: bash

    # Running the script with unidist on sequential Python backend
    $ unidist script.py --backend python

For more information on the CLI arguments specific to the Python backend
see :doc:`unidist CLI </using_cli>` section.

Second, setting the environment variable:

.. code-block:: bash

    # unidist will use sequential Python backend to do computations
    export UNIDIST_BACKEND=python

For more information on the environment variables and associated configs specific to the Python backend
see :doc:`config API </flow/unidist/config>` section.

.. note::
   Note that the config ``Backend`` object associated with the ``UNIDIST_BACKEND`` environment variable
   doesn't make sense to use in your code to set the execution backend since the environment variable and
   the argument of unidist CLI supersede that value.
