..
      Copyright (C) 2021-2022 Modin authors

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

  pip install unidist # Install unidist with dependencies for Multiprocessing and sequential Python backends

unidist can also be used with Dask, MPI or Ray execution backend.
If you don't have Dask_, MPI_ or Ray_ installed, you will need to install unidist with one of the targets:

.. code-block:: bash

  pip install unidist[all] # Install unidist with dependencies for all the backends
  pip install unidist[dask] # Install unidist with dependencies for Dask backend
  pip install unidist[mpi] # Install unidist with dependencies for MPI backend
  pip install unidist[ray] # Install unidist with dependencies for Multiprocessing and sequential Python backends

unidist automatically detects which execution backends are installed and uses that for
scheduling computation!

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

+---------------------------------+-----------------------------------------------------+-----------------------------+
| **Package name in conda-forge** | **Backend(s)**                                      | **Supported OSs**           |
+---------------------------------+-----------------------------------------------------+-----------------------------+
| unidist                         | `Multiprocessing`_, Python                          | Linux, Windows, MacOS       |
+---------------------------------+-----------------------------------------------------+-----------------------------+
| unidist-all                     | `Dask`_, `MPI`_, `Ray`_, `Multiprocessing`_, Python | Linux, Windows              |
+---------------------------------+-----------------------------------------------------+-----------------------------+
| unidist-dask                    | `Dask`_                                             | Linux, Windows, MacOS       |
+---------------------------------+-----------------------------------------------------+-----------------------------+
| unidist-mpi                     | `MPI`_                                              | Linux, Windows, MacOS       |
+---------------------------------+-----------------------------------------------------+-----------------------------+
| unidist-ray                     | `Ray`_                                              | Linux, Windows              |
+---------------------------------+-----------------------------------------------------+-----------------------------+

Installing unidist packages from the conda-forge channel can be achieved by adding conda-forge to your channels with:

.. code-block:: bash

  conda config --add channels conda-forge
  conda config --set channel_priority strict

For installing unidist with dependencies for Dask and MPI execution backends into a conda environment
the following command should be used:

.. code-block:: bash

  conda install unidist-dask unidist-mpi -c conda-forge

All set of backends could be available in a conda environment by specifying:

.. code-block:: bash

  conda install unidist-all -c conda-forge

or explicitly:

.. code-block:: bash

  conda install unidist-dask unidist-mpi unidist-ray -c conda-forge

Installing from the GitHub master branch
''''''''''''''''''''''''''''''''''''''''

If you'd like to try unidist using the most recent updates from the master branch, you can
also use ``pip``.

.. code-block:: bash

  # Install unidist with dependencies for Multiprocessing and sequential Python backends
  pip install git+https://github.com/modin-project/unidist
  # Install unidist with dependencies for all the backends
  pip install git+https://github.com/modin-project/unidist#egg=unidist[all]
  # Install unidist with dependencies for Ray backend
  pip install git+https://github.com/modin-project/unidist#egg=unidist[ray]

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
  # Install unidist with dependencies for Multiprocessing and sequential Python backends
  pip install -e .
  # Install unidist with dependencies for all the backends
  pip install -e .[all]
  # Install unidist with dependencies for Ray backend
  pip install -e .[ray]

.. _`GitHub repo`: https://github.com/modin-project/unidist/tree/master
.. _`issue`: https://github.com/modin-project/unidist/issues
.. _`Ray`: https://docs.ray.io/en/master/index.html
.. _`Dask`: https://distributed.dask.org/en/latest/
.. _`Multiprocessing`: https://docs.python.org/3/library/multiprocessing.html
.. _`MPI`: https://www.mpi-forum.org/
.. _`Semantic Versioning`: https://semver.org
