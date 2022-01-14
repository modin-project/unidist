..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

Unidist Command Line Interface
""""""""""""""""""""""""""""""

unidist provides its own command line interface (CLI) for running python applications.
The CLI is a recommended way for executing programs with unidist.

The CLI can be used in two ways. If unidist is installed in user's environment (from conda, pip or sources) the user
has *unidist* executable binary. In this case a recommended way to run an application is the following:

.. code-block:: bash

   $ unidist script.py

In the case of using unidist sources, the application can be run as follows:

.. code-block:: bash

   $ python unidist/cli script.py  # It is supposed the run happens from the unidist's root directory

CLI Options
===========

unidist behavior can be tuned via CLI options. All actual supported options can be got using ``--help``:

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

* Default is running the script with unidist on Ray backend in a single node:

  .. code-block:: bash

      $ unidist script.py

* Running the script with unidist on Dask backend via ``pytest`` with a pytest-specific option:

  .. code-block:: bash

      $ unidist -m pytest script.py -b dask --verbose  # --verbose is the pytest-specific option

  or (use ``UNIDIST_BACKEND`` environment variable instead of ``-b`` option):

  .. code-block:: bash

      $ export UNIDIST_BACKEND=Dask
      $ unidist -m pytest script.py --verbose  # --verbose is the pytest-specific option

* Running the script with unidist on MPI backend using 8 workers in a single node:

  .. code-block:: bash

      $ unidist script.py --backend mpi --num_cpus 8

* Running the script with unidist on MPI backend on two nodes. The nodes have 16 and 32 workers, respectively:

  .. code-block:: bash

      $ export UNIDIST_BACKEND=MPI
      $ unidist script.py -hosts localhost x.x.x.1 --num_cpus 16 32

* Running the script with unidist on Ray backend with a pre-initialized Ray cluster:

  .. code-block:: bash

      $ unidist script.py -hosts x.x.x.1 -redis_pswd 123456 # x.x.x.1 is the IP-address of a head node of the Ray cluster

* Running the script with unidist on Dask backend with a pre-initialized Dask cluster:

  .. code-block:: bash

      $ unidist script.py -b dask -hosts x.x.x.1:port # x.x.x.1:port is the IP-address with the port of a dask-scheduler

.. note:: 
    Currently, in order to use unidist with Ray or Dask backend on a cluster, Ray/Dask cluster needs to be pre-initialized
    using its own documentation (`Ray Guide <https://docs.ray.io/en/latest/cluster/cloud.html#manual-ray-cluster-setup>`_
    and `Dask Guide <https://docs.dask.org/en/latest/how-to/deploy-dask/cli.html>`_).
