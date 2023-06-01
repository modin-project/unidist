..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

MPI backend
===========

We highly encourage to use external (either custom-built or system-provided) MPI installations
in production scenarious to get ultimate performance.

Open MPI
--------

From source
"""""""""""

The following instructions will help you install Open MPI from source to use it as the unidist's backend.

1. Create a directory for compiling Open MPI and go into it. You can do this in a terminal by typing

.. code-block:: bash

  mkdir local
  cd local

2. Download ``openmpi-4.1.5.tar.bz2`` from http://www.open-mpi.org, e.g., using ``curl`` command

.. code-block:: bash

  curl -O https://download.open-mpi.org/release/open-mpi/v4.1/openmpi-4.1.5.tar.bz2

Note that we use the specific version of Open MPI as an example. You can install any version you want.

3. Extract the package using

.. code-block:: bash

  tar -jxf openmpi-4.1.5.tar.bz2

4. Go into the source directory

.. code-block:: bash

  cd openmpi-4.1.5

5. Configure, compile and install by executing the following commands

.. code-block:: bash

  ./configure --prefix=path/to/local/<openmpi>
  make all
  make install

This will install Open MPI in ``local/openmpi`` directory. You can speed up
the compilation by replacing the ``make all`` command with ``make -j4 all``
(this will compile using 4 cores).

6. Remove the temporary directories (optional):

.. code-block:: bash

  rm path/to/local/openmpi-4.1.5.tar.bz2
  rm -rf path/to/local/openmpi-4.1.5

To use the installed Open MPI you will have to adapt your ``PATH`` and ``MPICC`` environment variables.

.. code-block:: bash

  export PATH=path/to/local/openmpi/bin:$PATH

Then, you can install ``unidist`` and the required dependencies for the MPI backend.

.. code-block:: bash
  pip install mpi4py
  pip install msgpack
  pip install unidist

Now you can use unidist on MPI backend using Open MPI implementaion.

MPICH
-----

From source
"""""""""""

The following instructions will help you install MPICH from source to use it as the unidist's backend.

1. Create a directory for compiling MPICH and go into it. You can do this in a terminal by typing

.. code-block:: bash

  mkdir local
  cd local

2. Download ``mpich-4.1.1.tar.gz`` from https://www.mpich.org, e.g., using ``curl`` command

.. code-block:: bash

  curl -O https://www.mpich.org/static/downloads/4.1.1/mpich-4.1.1.tar.gz

Note that we use the specific version of MPICH as an example. You can install any version you want.

3. Extract the package using

.. code-block:: bash

  tar -xzvf mpich-4.1.1.tar.gz

4. Go into the source directory

.. code-block:: bash

  cd mpich-4.1.1

5. Configure, compile and install by executing the following commands

.. code-block:: bash

  ./configure --prefix=path/to/local/<mpich>
  make all
  make install

This will install Open MPI in ``local/mpich`` directory. You can speed up
the compilation by replacing the ``make all`` command with ``make -j4 all``
(this will compile using 4 cores).

6. Remove the temporary directories (optional):

.. code-block:: bash

  rm path/to/local/mpich-4.1.1.tar.gz
  rm -rf path/to/local/mpich-4.1.1

To use the installed MPICH you will have to adapt your ``PATH`` and ``MPICC`` environment variables.

.. code-block:: bash

  export PATH=path/to/local/mpich/bin:$PATH

Then, you can install ``unidist`` and the required dependencies for the MPI backend.

.. code-block:: bash
  pip install mpi4py
  pip install msgpack
  pip install unidist

Now you can use unidist on MPI backend using MPICH implementaion.

Intel MPI
---------

From Intel oneAPI HPC Toolkit
"""""""""""""""""""""""""""""

The following instructions will help you install Intel MPI from `Intel oneAPI HPC Toolkit`_ to use it as the unidist's backend.
We will use an offline installer an an example but you are free to use other installation options.

1. Create a directory for installing Intel MPI and go into it. You can do this in a terminal by typing

.. code-block:: bash

  mkdir local
  cd local

2. Download a toolkit installer from `Intel oneAPI HPC Toolkit`_, e.g., using ``wget`` command

.. code-block:: bash

  wget https://registrationcenter-download.intel.com/akdlm/IRC_NAS/1ff1b38a-8218-4c53-9956-f0b264de35a4/l_HPCKit_p_2023.1.0.46346_offline.sh

Note that we use the specific version of the toolkit as an example. You can install any version you want.

3. Launch the installer

.. code-block:: bash

  sh ./l_HPCKit_p_2023.1.0.46346_offline.sh

During installation process you can choose a directory in which the toolkit should be installed
(e.g., ``local/path/to/toolkit``).

4. Source the ``setvars.sh`` (global to the toolkit) or the ``vars.sh`` (local to the Intel MPI)

.. code-block:: bash

  # source local/path/to/toolkit/oneapi/setvars.sh
  source local/path/to/toolkit/oneapi/mpi/latest/env/vars.sh

5. Remove the installer (optional):

.. code-block:: bash

  rm l_HPCKit_p_2023.1.0.46346_offline.sh


6. Install ``unidist`` and the required dependencies for the MPI backend.

.. code-block:: bash
  pip install mpi4py
  pip install msgpack
  pip install unidist

Now you can use unidist on MPI backend using Intel MPI implementaion.

.. _`Intel oneAPI HPC Toolkit`: https://www.intel.com/content/www/us/en/developer/tools/oneapi/hpc-toolkit-download.html
