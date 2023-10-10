..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

MPI backend
===========

We highly encourage to use external (either custom-built or system-provided) MPI installations
in production scenarious to get ultimate performance.

Open Source MPI implementaions
------------------------------

Building MPI from source
""""""""""""""""""""""""

In `Building MPI from source`_ section of ``mpi4py`` documentation you can find executive instructions
for building some of the open-source MPI implementations out there with support for shared/dynamic libraries on POSIX environments.

Once you have a working MPI implementation, you will have to adapt your ``PATH`` environment variable
to use the installed MPI version.

.. code-block:: bash

  export PATH=path/to/mpi/bin:$PATH

Then, you can install ``unidist`` and the required dependencies for the MPI backend.

.. code-block:: bash

  pip install unidist[mpi]

Now you can use unidist on MPI backend using the installed MPI implementaion.

Proprietary MPI implementations
-------------------------------

Intel MPI From Intel oneAPI HPC Toolkit
"""""""""""""""""""""""""""""""""""""""

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
(e.g., ``path/to/local/<toolkit>``).

4. Source the ``setvars.sh`` (global to the toolkit) or the ``vars.sh`` (local to the Intel MPI)

.. code-block:: bash

  # source path/to/local/<toolkit>/oneapi/setvars.sh
  source path/to/local/<toolkit>/oneapi/mpi/latest/env/vars.sh

5. Install ``unidist`` and the required dependencies for the MPI backend

.. code-block:: bash

  pip install unidist[mpi]

Now you can use unidist on MPI backend using Intel MPI implementaion.

6. Remove the installer (optional):

.. code-block:: bash

  rm l_HPCKit_p_2023.1.0.46346_offline.sh

.. _`Intel oneAPI HPC Toolkit`: https://www.intel.com/content/www/us/en/developer/tools/oneapi/hpc-toolkit-download.html
.. _`Building MPI from source`: https://mpi4py.readthedocs.io/en/latest/appendix.html#building-mpi-from-sources
