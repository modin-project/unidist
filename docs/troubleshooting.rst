..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

Troubleshooting
===============

We hope your experience with Unidist is bug-free, but there are some quirks about Unidist 
that may require troubleshooting. If you are still having issues, please open a Github 
`issue`_.

Frequently encountered issues
-----------------------------

This is a list of the most frequently encountered issues when using Unidist. Some of these 
are working as intended, while others are known bugs that are being actively worked on.

Error when using Open MPI while running in a cluster: ``bash: line 1: orted: command not found``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Sometimes, when you run a program with Open MPI in a cluster, you may see the following error:

.. code-block:: bash

  bash: line 1: orted: command not found
  --------------------------------------------------------------------------
  ORTE was unable to reliably start one or more daemons.
  This usually is caused by:
  
  * not finding the required libraries and/or binaries on
    one or more nodes. Please check your PATH and LD_LIBRARY_PATH
    settings, or configure OMPI with --enable-orterun-prefix-by-default
  
  * lack of authority to execute on one or more specified nodes.
    Please verify your allocation and authorities.
  
  * the inability to write startup files into /tmp (--tmpdir/orte_tmpdir_base).
    Please check with your sys admin to determine the correct location to use.
  
  *  compilation of the orted with dynamic libraries when static are required
    (e.g., on Cray). Please check your configure cmd line and consider using
    one of the contrib/platform definitions for your system type.
  
  * an inability to create a connection back to mpirun due to a
    lack of common network interfaces and/or no route found between
    them. Please check network connectivity (including firewalls
    and network routing requirements).
  --------------------------------------------------------------------------

**Solution**

You should add the ``--prefix`` parameter to the ``mpiexec`` command with the path to the installed 
Open MPI library. If you are using a conda environment, then the required path will be: 
``$CONDA_PATH/envs/<ENV_NAME>``.

.. code-block:: bash

  mpiexec -n 1 --prefix $CONDA_PATH/envs/<ENV_NAME> python script.py


Error when using Open MPI while running in a cluster: ``OpenSSL version mismatch. Built against 30000020, you have 30100010``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Sometimes, when you run a program with Open MPI in a cluster, you may see the following error:

.. code-block:: bash

  OpenSSL version mismatch. Built against 30000020, you have 30100010
  --------------------------------------------------------------------------
  ORTE was unable to reliably start one or more daemons.
  This usually is caused by:
  
  * not finding the required libraries and/or binaries on
    one or more nodes. Please check your PATH and LD_LIBRARY_PATH
    settings, or configure OMPI with --enable-orterun-prefix-by-default
  
  * lack of authority to execute on one or more specified nodes.
    Please verify your allocation and authorities.
  
  * the inability to write startup files into /tmp (--tmpdir/orte_tmpdir_base).
    Please check with your sys admin to determine the correct location to use.
  
  *  compilation of the orted with dynamic libraries when static are required
    (e.g., on Cray). Please check your configure cmd line and consider using
    one of the contrib/platform definitions for your system type.
  
  * an inability to create a connection back to mpirun due to a
    lack of common network interfaces and/or no route found between
    them. Please check network connectivity (including firewalls
    and network routing requirements).
  --------------------------------------------------------------------------

This may happen due to the fact that OpenMPI uses OpenSSH
but its version is built on a different version of OpenSSL than yours.

**Solution**

You should check for version compatibility of OpenSSH and OpenSSL and update them if necessary.

.. code-block:: bash

  $ openssl version
  OpenSSL 3.0.9 30 May 2023 (Library: OpenSSL 3.0.9 30 May 2023)
  $ ssh -V
  OpenSSH_8.9p1 Ubuntu-3ubuntu0.1, OpenSSL 3.0.2 15 Mar 2022

If you use ``conda``, just add ``openssh`` library to your environment.

.. code-block:: bash

  conda install -c conda-forge openssh


Error when using MPI backend: ``mpi4py.MPI.Exception: MPI_ERR_SPAWN: could not spawn processes``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This error usually happens on Open MPI when you try to start the number of workers exceeding the number of physical cores.
Open MPI binds workers to physical cores by default.

.. code-block:: bash

  mpi4py.MPI.Exception: MPI_ERR_SPAWN: could not spawn processes
  --------------------------------------------------------------------------
  Primary job  terminated normally, but 1 process returned
  a non-zero exit code. Per user-direction, the job has been aborted.
  --------------------------------------------------------------------------
  --------------------------------------------------------------------------
  mpiexec detected that one or more processes exited with non-zero status, thus causing
  the job to be terminated. The first process to do so was:

    Process name: [[35427,1],0]
    Exit code:    1
  --------------------------------------------------------------------------

**Solution**

You should add one of the flags below to ``mpiexec`` command when running your application.

* ``--bind-to hwthread``
* ``--use-hwthread-cpus``
* ``--oversubscribe``

.. code-block:: bash

  mpiexec -n 1 --bind-to hwthread python script.py

To get more information about the flags refer to `Open MPI's mpiexec`_ command documentation.

Error when using MPI backend: ``There are not enough slots available in the system to satisfy the <N> slots``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This error usually happens on Open MPI when you try to start the number of workers exceeding the number of physical cores.
Open MPI binds workers to physical cores by default.

.. code-block:: bash

  --------------------------------------------------------------------------
  There are not enough slots available in the system to satisfy the <N>
  slots that were requested by the application:

    python

  Either request fewer slots for your application, or make more slots
  available for use.

  A "slot" is the Open MPI term for an allocatable unit where we can
  launch a process.  The number of slots available are defined by the
  environment in which Open MPI processes are run:

    1. Hostfile, via "slots=N" clauses (N defaults to number of
      processor cores if not provided)
    2. The --host command line parameter, via a ":N" suffix on the
      hostname (N defaults to 1 if not provided)
    3. Resource manager (e.g., SLURM, PBS/Torque, LSF, etc.)
    4. If none of a hostfile, the --host command line parameter, or an
      RM is present, Open MPI defaults to the number of processor cores

  In all the above cases, if you want Open MPI to default to the number
  of hardware threads instead of the number of processor cores, use the
  --use-hwthread-cpus option.

  Alternatively, you can use the --oversubscribe option to ignore the
  number of available slots when deciding the number of processes to
  launch.
  --------------------------------------------------------------------------

**Solution**

You should add one of the flags below to ``mpiexec`` command when running your application to allow Open MPI
to start the number of workers exceeding the number of physical cores.

* ``--bind-to hwthread``
* ``--use-hwthread-cpus``
* ``--oversubscribe``

.. code-block:: bash

  mpiexec -n 1 --bind-to hwthread python script.py

To get more information about the flags refer to `Open MPI's mpiexec`_ command documentation.

.. _`Open MPI's mpiexec`: https://www.open-mpi.org/doc/v3.1/man1/mpiexec.1.php
.. _`issue`: https://github.com/modin-project/unidist/issues


Shared object store is not supported in C/W model if the using MPICH version is less than the 4.2.0 version.
------------------------------------------------------------------------------------------------------------
Unfortunately, this version of MPICH has a problem with shared memory in the Controller/Worker model.

**Solution**
You can run your script using the SPMD model, or use other MPI implementations 
such as Open MPI, Intel MPI, or MPICH above version 4.2.0.