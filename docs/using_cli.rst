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

   $ python unidist/cli script.py  # The running is happened from a unidist's root directory.


CLI Options
===========

Unidist behavior can be tuned via CLI options. All actual supported options can always be obtained using ``--help``:

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


Usage Examples
==============

Suppose that user has file ``script.py`` with unidist functionality. Consider several cases of running this script with
different options/environment variables combinations.

* Default is running in a single node mode on Ray backend:

  .. code-block:: bash

      $ unidist script.py

* Running ``pytest`` on Dask backend with a pytest-specific option:

  .. code-block:: bash

      $ unidist -m pytest script.py -b dask --verbose  # --verbose is pytest-specific option

  or (use ``UNIDIST_BACKEND`` environment variable instead of ``-b`` option):

  .. code-block:: bash

      $ export UNIDIST_BACKEND=Dask
      $ unidist -m pytest script.py --verbose  # --verbose is pytest-specific option

* Running in a single node mode on MPI backend using 8 workers:

  .. code-block:: bash

      $ unidist script.py --backend mpi --num_cpus 8

* Running the script on two nodes using MPI backend. Nodes will have 16 and 32 workers, respectively:

  .. code-block:: bash

      $ export UNIDIST_BACKEND=MPI
      $ unidist script.py -hosts localhost x.x.x.1 --num_cpus 16 32

* Running the script on a Ray pre-initialized cluster:

  .. code-block:: bash

      $ unidist script.py -hosts x.x.x.1 -redis_pswd 123456 # x.x.x.1 is IP-address of a head node of Ray cluster

* Running the script on a Dask pre-initialized cluster:

  .. code-block:: bash

      $ unidist script.py -b dask -hosts x.x.x.1:port # x.x.x.1:port is IP-address with port of a Dask-scheduler

.. note:: 
    Currently, to use unidist with Ray and Dask backends on cluster, need to pre-initialize Dask/Ray cluster
    using their own documentation (`Ray Guide <https://docs.ray.io/en/latest/starting-ray.html#starting-ray-via-the-cli-ray-start>`_
    and `Dask Guide <https://docs.dask.org/en/latest/how-to/deploy-dask-clusters.html>`_).
