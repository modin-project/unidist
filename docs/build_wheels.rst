..
      Copyright (C) 2021-2022 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Build wheels
""""""""""""

To build unidist wheels, use the following method:

.. code-block:: bash

    # Fresh clone unidist
    $ git clone git@github.com:modin-project/unidist.git
    $ cd unidist
    # Build wheels. Wheels must be built per-distribution
    $ SETUP_PLAT_NAME=macos python setup.py sdist bdist_wheel --plat-name macosx_10_9_x86_64
    $ SETUP_PLAT_NAME=win32 python setup.py sdist bdist_wheel --plat-name win32
    $ SETUP_PLAT_NAME=win_amd64 python setup.py sdist bdist_wheel --plat-name win_amd64
    $ SETUP_PLAT_NAME=linux python setup.py sdist bdist_wheel --plat-name manylinux1_x86_64
    $ SETUP_PLAT_NAME=linux python setup.py sdist bdist_wheel --plat-name manylinux1_i686

You may see the wheels in the ``dist`` folder: ``ls -l dist``. Make sure the version is correct,
and make sure there are 5 distributions listed above with the ``--plat-name`` arguments.
Also make sure there is a ``tar`` file that contains the source.

Upload wheels
"""""""""""""

Use ``twine`` to upload wheels:

.. code-block:: bash

    $ twine upload dist/*

Check with ``pip install``
""""""""""""""""""""""""""

Run ``pip install -U unidist[all]`` on Linux and Windows systems in a new environment
to test that the wheels were uploaded correctly.
