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

.. _`issue`: https://github.com/modin-project/unidist/issues
