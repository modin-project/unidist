..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

Unidist Command Line Interface
""""""""""""""""""""""""""""""

Unidist provides its own command line interface (CLI) for running applications.
The CLI is recommended for an executing unidist programs. 

The CLI can be used in two ways. If unidist is installed in user's environment (from conda, pip or sources) user will have
*unidist* executable binary. In this case a recommended way to run applications is the next:

.. code-block:: bash

   $ unidist script.py

In a case of using unidist from sources, the applications can be run by the next way:

.. code-block:: bash

   $ python unidist/cli script.py  # The running is happen from a unidist's root directory.


CLI Options
-----------

Unidist behavior can be changed via CLI options. All actual supported options can always be obtained using ``--help``:

.. code-block::

   $ unidist --help
   usage:
        In case 'unidist' is installed as a python package, run binary:
        unidist script.py  # Ray backend is used by default
        unidist script.py --backend mpi  # MPI backend is used
        unidist -m pytest script.py -b dask  # Dask backend is used, running the script using 'pytest'
        unidist script.py -b multiprocessing --num_cpus 16 # MultiProcessing backend is used and uses 16 CPUs

        To run from sources run 'unidist/cli':
        python unidist/cli script.py -b mpi --num_cpus 16 -hosts localhost  # MPI backend uses 16 workers on 'localhost' node
        python unidist/cli script.py -b mpi -num_cpus 2 4 --hosts localhost x.x.x.x  # MPI backend uses 2 workers on 'localhost' and 4 on 'x.x.x.x'

   Run python code with 'unidist'.

   optional arguments:
   -h, --help            show this help message and exit
   -b {ray,mpi,dask,multiprocessing,python}, --backend {ray,mpi,dask,multiprocessing,python}
                           specify an execution backend. Default value is taken from 'UNIDIST_BACKEND' environment
                           variable. If 'UNIDIST_BACKEND' isn't set, 'ray' backend is used
   -m MODULE, --module MODULE
                           specify a python module to run your script with
   -num_cpus NUM_CPUS [NUM_CPUS ...], --num_cpus NUM_CPUS [NUM_CPUS ...]
                           specify a number of CPUs per node used by the backend in a cluster. Can accept multiple
                           values in the case of running in the cluster. Default value is taken from
                           'UNIDIST_CPUS' environment variable. If 'UNIDIST_CPUS' isn't set, value is equal to the
                           number of CPUs on a head node.
   -hosts HOSTS [HOSTS ...], --hosts HOSTS [HOSTS ...]
                           specify node(s) IP address(es) to use by the backend. Can accept multiple values in the
                           case of running in a cluster. Default is 'localhost'.

   required arguments:
   script                specify a script to be run

   Ray backend-specific arguments:
   -redis_pswd REDIS_PASSWORD, --redis_password REDIS_PASSWORD
                           specify redis password to connect to existing Ray cluster.
