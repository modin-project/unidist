..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

============
Installation
============

There are several ways to install unidist. Most users will want to install with
``pip`` or using ``conda`` tool, but some users may want to build from the master branch
on the `GitHub repo`_. The master branch has the most recent patches, but may be less
stable than a release installed from ``pip`` or ``conda``.

Installing with pip
'''''''''''''''''''

Stable version
""""""""""""""

unidist can be installed with ``pip`` on Linux, Windows and MacOS.
To install the most recent stable release run the following:

.. code-block:: bash

  pip install unidist # Install unidist with dependencies for Python Multiprocessing and Python Sequential backends

unidist can also be used with MPI, Dask or Ray execution backend.
If you don't have MPI_, Dask_ or Ray_ installed, you will need to install unidist with one of the targets:

.. code-block:: bash

  pip install unidist[all] # Install unidist with dependencies for all the backends
  pip install unidist[mpi] # Install unidist with dependencies for MPI backend
  pip install unidist[dask] # Install unidist with dependencies for Dask backend
  pip install unidist[ray] # Install unidist with dependencies for Ray backend

unidist automatically detects which execution backends are installed and uses that for
scheduling computation!

.. note::
    There are different MPI implementations, each of which can be used as a backend in unidist.
    Mapping ``unidist[mpi]`` installs ``mpi4py`` package, which is just a Python wrapper for MPI.
    To enable unidist on MPI execution you need to have a working MPI implementation and certain software installed.
    Refer to installation_ page of the `mpi4py` documentation for details.
    Also, you can find some instructions on :doc:`MPI backend </optimization_notes/mpi>` page.

Release candidates
""""""""""""""""""

unidist follows `Semantic Versioning`_ and before some major or minor releases,
we will upload a release candidate to test and check if there are any problems.
If you would like to install a pre-release of unidist, run the following:

.. code-block:: bash

  pip install --pre unidist

These pre-releases are uploaded for dependencies and users to test their existing code
to ensure that it still works. If you find something wrong, please raise an issue_.

Installing with conda
'''''''''''''''''''''

Using conda-forge channel
"""""""""""""""""""""""""

unidist releases can be installed using ``conda`` from the conda-forge channel. Starting from the first 0.1.0 release
it is possible to install unidist with chosen execution backend(s) alongside. Current options are:

+---------------------------------+-----------------------------------------------------------------------+-----------------------------+
| **Package name in conda-forge** | **Backend(s)**                                                        | **Supported OSs**           |
+---------------------------------+-----------------------------------------------------------------------+-----------------------------+
| unidist                         | `Python Multiprocessing`_, Python Sequential                          | Linux, Windows, MacOS       |
+---------------------------------+-----------------------------------------------------------------------+-----------------------------+
| unidist-all                     | `MPI`_, `Dask`_, `Ray`_, `Python Multiprocessing`_, Python Sequential | Linux, Windows              |
+---------------------------------+-----------------------------------------------------------------------+-----------------------------+
| unidist-mpi                     | `MPI`_                                                                | Linux, Windows, MacOS       |
+---------------------------------+-----------------------------------------------------------------------+-----------------------------+
| unidist-dask                    | `Dask`_                                                               | Linux, Windows, MacOS       |
+---------------------------------+-----------------------------------------------------------------------+-----------------------------+
| unidist-ray                     | `Ray`_                                                                | Linux, Windows              |
+---------------------------------+-----------------------------------------------------------------------+-----------------------------+

For installing unidist with dependencies for MPI and Dask execution backends into a conda environment
the following command should be used:

.. code-block:: bash

  conda install unidist-mpi unidist-dask -c conda-forge

All set of backends could be available in a conda environment by specifying:

.. code-block:: bash

  conda install unidist-all -c conda-forge

or explicitly:

.. code-block:: bash

  conda install unidist-mpi unidist-dask unidist-ray -c conda-forge

.. note:: 
    There are different MPI implementations, each of which can be used as a backend in unidist.
    By default, mapping ``unidist-mpi`` installs a default MPI implementation, which comes with ``mpi4py`` package and is ready to use.
    The conda dependency solver decides on which MPI implementation is to be installed. If you want to use a specific version of MPI,
    you can install the core dependencies for MPI backend and the specific version of MPI as ``conda install unidist-mpi <mpi>``
    as shown in the installation_ page of ``mpi4py`` documentation. That said, it is highly encouraged to use your own MPI binaries
    as stated in the `Using External MPI Libraries`_ section of the conda-forge documentation in order to get ultimate performance.

Using intel channel
"""""""""""""""""""

Conda ``intel`` channel contains a performant `MPI implementaion <https://anaconda.org/intel/mpi4py>`_,
which can be used in the unidist MPI backend instead of an MPI implementation from ``conda-forge`` channel.
To install Intel MPI you should use the following:

.. code-block:: bash

  conda install unidist -c conda-forge
  conda install mpi4py -c intel

Installing from the GitHub master branch
''''''''''''''''''''''''''''''''''''''''

If you'd like to try unidist using the most recent updates from the master branch, you can
also use ``pip``.

.. code-block:: bash

  # Install unidist with dependencies for Python Multiprocessing and Python Sequential backends
  pip install git+https://github.com/modin-project/unidist
  # Install unidist with dependencies for all the backends
  pip install git+https://github.com/modin-project/unidist#egg=unidist[all]
  # Install unidist with dependencies for MPI backend
  pip install git+https://github.com/modin-project/unidist#egg=unidist[mpi]

This will install directly from the repo without you having to manually clone it! Please be aware
that these changes have not made it into a release and may not be completely stable.

Building unidist from Source
''''''''''''''''''''''''''''

If you're planning to :doc:`contribute </developer/contributing>` to unidist, you need to ensure that you are
building unidist from the local repository that you are working of. Occasionally,
there are issues in overlapping unidist installs from PyPI and from source. To avoid these
issues, we recommend uninstalling unidist before installation from source:

.. code-block:: bash

  pip uninstall unidist

To build from source, you first must clone the repo. We recommend forking the repository first
through the GitHub interface, then cloning as follows:

.. code-block:: bash

  git clone https://github.com/<your-github-username>/unidist.git

Once cloned, ``cd`` into the ``unidist`` directory and use ``pip`` to install:

.. code-block:: bash

  cd unidist
  # Install unidist with dependencies for Python Multiprocessing and Python Sequential backends
  pip install -e .
  # Install unidist with dependencies for all the backends
  pip install -e .[all]
  # Install unidist with dependencies for MPI backend
  pip install -e .[mpi]

.. _`GitHub repo`: https://github.com/modin-project/unidist/tree/master
.. _`issue`: https://github.com/modin-project/unidist/issues
.. _`Ray`: https://docs.ray.io/en/master/index.html
.. _`Dask`: https://distributed.dask.org/en/latest/
.. _`Python Multiprocessing`: https://docs.python.org/3/library/multiprocessing.html
.. _`MPI`: https://www.mpi-forum.org/
.. _`Semantic Versioning`: https://semver.org
.. _`installation`: https://mpi4py.readthedocs.io/en/latest/install.html
.. _`Using External MPI Libraries`: https://conda-forge.org/docs/user/tipsandtricks.html#using-external-message-passing-interface-mpi-libraries
