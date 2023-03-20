..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

:orphan:

Build wheels
""""""""""""

To build unidist wheels, use the following method:

.. code-block:: bash

    # Fresh clone unidist
    $ git clone git@github.com:modin-project/unidist.git
    $ cd unidist
    # Build a pure Python wheel
    $ python setup.py sdist bdist_wheel

You may see the wheel in the `dist` folder: `ls -l dist`. Make sure the version is correct.
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
