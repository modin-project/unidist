..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist on MPI
''''''''''''''

This section describes the use of unidist with the MPI execution backend.

There are two ways to choose the execution backend to run on.
First, the recommended way is to use the argument of unidist CLI:

.. code-block:: bash

    # Running the script with unidist on MPI backend
    $ unidist script.py --backend mpi

For more information on the CLI arguments specific to the MPI backend
see :doc:`unidist CLI </using_cli>` section.

Second, setting the environment variable:

.. code-block:: bash

    # unidist will use MPI backend to distribute computations
    export UNIDIST_BACKEND=mpi

For more information on the environment variables and associated configs specific to the MPI backend
see :doc:`config API </flow/unidist/config>` section.

.. note::
   Note that the config ``Backend`` and ``CpuCount`` objects associated with the ``UNIDIST_BACKEND`` and
   ``UNIDIST_CPUS`` environment variables, respectively, do not make sense to use in your code
   to set the execution backend and number of CPUs since the environment variables and
   the arguments of unidist CLI supersede that value.
