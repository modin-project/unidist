..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Unidist on MPI
''''''''''''''

This section describes the use of unidist with the MPI execution backend.
Since there are different MPI implementations, each of which can be used as a backend in unidist,
refer to :doc:`Installation </installation>` page on how to install a specific MPI implementation.

There are two ways to choose the execution backend to run on.
First, by setting the ``UNIDIST_BACKEND`` environment variable:

.. code-block:: bash

    # unidist will use MPI
    $ export UNIDIST_BACKEND=mpi

.. code-block:: python

    import os

    # unidist will use MPI
    os.environ["UNIDIST_BACKEND"] = "mpi"

Second, by setting the configuration value associated with the environment variable:

.. code-block:: python

    from unidist.config import Backend

    Backend.put("mpi")  # unidist will use MPI

For more information on the environment variables and associated configs specific to the MPI backend
see :doc:`config API </flow/unidist/config>` section.

Run unidist on MPI
''''''''''''''''''

Unidist on MPI in a single node
"""""""""""""""""""""""""""""""

In order to run unidist on MPI in a single node, there are two options.

Controller/Worker model
-----------------------

This execution model is similar to ones other execution backends use.
To run unidist on MPI in a single node using Controller/Worker model you should use ``mpiexec -n 1 python <script.py>`` command.

.. code-block:: bash

    $ mpiexec -n 1 python script.py

MPI worker processes will be spawned dynamically by unidist.

It is worth noting that some MPI implementations, e.g., `Intel MPI implementation`_, support the ability of spawning MPI processes
without using ``mpiexec`` command so you can run unidist on Intel MPI just with:

.. code-block:: bash

    $ python script.py

Refer to ``Using intel channel`` section of :doc:`Installation </installation>` page on how to install Intel MPI implementation to use it with unidist.

SPMD model
----------

First of all, to run unidist on MPI in a single node using `SPMD model`_,
you should set the ``UNIDIST_IS_MPI_SPAWN_WORKERS`` environment variable to ``False``:

.. code-block:: bash

    $ export UNIDIST_IS_MPI_SPAWN_WORKERS=False

.. code-block:: python

    import os

    os.environ["UNIDIST_IS_MPI_SPAWN_WORKERS"] = "False"

or set the associated configuration value:

.. code-block:: python

    from unidist.config import IsMpiSpawnWorkers

    IsMpiSpawnWorkers.put(False)

This will enable unidist not to spawn MPI processes dynamically because the user himself spawns the processes.

Then, you should also use ``mpiexec`` command and specify a number of workers to spawn.

.. code-block:: bash

    $ mpiexec -n N python script.py

When initializing unidist this execution model gets transformed to Controller/Worker model.

.. note:: 
    Note that the process with rank 0 devotes for the controller (master) process you interact with,
    the process with rank 1 devotes for the monitor process unidist on MPI uses for tracking executed tasks.
    So the processes with ranks 2 to N devote for worker processes where computation will be executed.
    If you right away use Controller/Worker model to run unidist on MPI, this happens transparently.

Unidist on MPI cluster
""""""""""""""""""""""

In order to run unidist on MPI in a cluster, there are two options.

Controller/Worker model
-----------------------

This execution model is similar to ones other execution backends use.
In order to run unidist on MPI in a cluster you should specify hosts to run on.
There are two ways to specify MPI hosts to run on.

First, by setting the ``UNIDIST_MPI_HOSTS`` environment variable:

.. code-block:: bash

    # unidist will use the hosts to run on
    $ export UNIDIST_MPI_HOSTS=<host1>,...,<hostN>

.. code-block:: python

    import os

    # unidist will use the hosts to run on
    os.environ["UNIDIST_MPI_HOSTS"] = "<host1>,...,<hostN>"

Second, by setting the configuration value associated with the environment variable:

.. code-block:: python

    from unidist.config import MpiHosts

    MpiHosts.put("host1,...,hostN")  # unidist will use the hosts to run on


Then, if you running without ``mpiexec`` command it is enough and the running is the same as in a single node.

.. note::
    Root proccess will allways be executed locally and other proccesses will be spawned in order on the specified hosts.
    If you want to run root process on a remote host, you should use `ssh host` before the command and carefully check that the environment is correct. 
    You can set some variables in ssh command or activate the conda envirenment right before running the Python script:

.. code-block:: bash

    ssh host ENV_VARIABLE=value "source $CONDA_PATH/bin/activate $CONDA_ENV; cd $PWD; python script.py"

Running is almost the same as in a single node, but, in addition,
you should use the appropriate parameter for ``mpiexec``.
This parameter differs depending on the MPI implementation used.

* For Intel MPI or MPICH: ``-hosts host1,host2``.
  You can also see `Controlling Process Placement with the Intel® MPI Library`_  or
  `MPICH wiki`_ for deeper customization.
* For OpenMPI: ``-host host1:n1,...,hostM:nM``, where n1, ..., nM is the number of processes on each node,
  including unidist service processes (root and monitor(s)).
  You can also see `Scheduling processes across hosts with OpenMPI Library`_ for deeper customization. 

SPMD model
""""""""""

First of all, to run unidist on MPI in a cluster using `SPMD model <https://en.wikipedia.org/wiki/Single_program,_multiple_data>`_,
you should set the ``UNIDIST_IS_MPI_SPAWN_WORKERS`` environment variable to ``False``:

.. code-block:: bash

    $ export UNIDIST_IS_MPI_SPAWN_WORKERS=False

.. code-block:: python

    import os

    os.environ["UNIDIST_IS_MPI_SPAWN_WORKERS"] = "False"

or set the associated configuration value:

.. code-block:: python

    from unidist.config import IsMpiSpawnWorkers

    IsMpiSpawnWorkers.put(False)

This will enable unidist not to spawn MPI processes dynamically because the user himself spawns the processes.

Then, you should use the appropriate parameter for ``mpiexec``.
This parameter differs depending on the MPI implementation used.

* For Intel MPI or MPICH: ``-hosts host1,...,hostM``. You can also see `Controlling Process Placement with the Intel® MPI Library`_ 
or `MPICH wiki`_ for deeper customization.

* For OpenMPI: ``-host host1:n1,...,hostM:nM``
where n1, ..., nM is the number of processes on each node, including Unidist service processes (root and one or some monitors).
You can also see `Scheduling processes across hosts with OpenMPI Library <https://docs.open-mpi.org/en/v5.0.x/launching-apps/scheduling.html>`_ for deeper customization. 

.. note:: 
    Note that the process with rank 0 devotes for the controller (master) process you interact with,
    the process with rank 1 devotes for the monitor process unidist on MPI uses for tracking executed tasks.
    So the processes with ranks 2 to N devote for worker processes where computation will be executed.
    If you right away use Controller/Worker model to run unidist on MPI, this happens transparently.

.. _`SPMD model`: https://en.wikipedia.org/wiki/Single_program,_multiple_data
.. _`Intel MPI implementation`: https://anaconda.org/intel/mpi4py
.. _`Controlling Process Placement with the Intel® MPI Library`: https://www.intel.com/content/www/us/en/developer/articles/technical/controlling-process-placement-with-the-intel-mpi-library.html
.. _`MPICH wiki`: https://github.com/pmodels/mpich/blob/main/doc/wiki/how_to/Using_the_Hydra_Process_Manager.md
.. _`Scheduling processes across hosts with OpenMPI Library`: https://docs.open-mpi.org/en/v5.0.x/launching-apps/scheduling.html
