..
      Copyright (C) 2021-2023 Modin authors

      SPDX-License-Identifier: Apache-2.0

Contributing
============

Certificate of Origin
---------------------

To keep a clear track of who did what, we use a `sign-off` procedure (same requirements
for using the signed-off-by process as the Linux kernel has
https://www.kernel.org/doc/html/v4.17/process/submitting-patches.html) on patches or pull
requests that are being sent. The sign-off is a simple line at the end of the explanation
for the patch, which certifies that you wrote it or otherwise have the right to pass it
on as an open-source patch. The rules are pretty simple: if you can certify the below:

CERTIFICATE OF ORIGIN V 1.1
^^^^^^^^^^^^^^^^^^^^^^^^^^^

"By making a contribution to this project, I certify that:

1.) The contribution was created in whole or in part by me and I have the right to
submit it under the open source license indicated in the file; or
2.) The contribution is based upon previous work that, to the best of my knowledge, is
covered under an appropriate open source license and I have the right under that license
to submit that work with modifications, whether created in whole or in part by me, under
the same open source license (unless I am permitted to submit under a different
license), as indicated in the file; or
3.) The contribution was provided directly to me by some other person who certified (a),
(b) or (c) and I have not modified it.
4.) I understand and agree that this project and the contribution are public and that a
record of the contribution (including all personal information I submit with it,
including my sign-off) is maintained indefinitely and may be redistributed consistent
with this project or the open source license(s) involved."

.. code-block:: bash

   This is my commit message

   Signed-off-by: Awesome Developer <developer@example.org>

Code without a proper signoff cannot be merged into the master branch.
Note: You must use your real name (sorry, no pseudonyms or anonymous contributions.)

The text can either be manually added to your commit body, or you can add either ``-s``
or ``--signoff`` to your usual ``git commit`` commands:

.. code-block:: bash

   git commit --signoff
   git commit -s

This will use your default git configuration which is found in .git/config. To change
this, you can use the following commands:

.. code-block:: bash

   git config --global user.name "Awesome Developer"
   git config --global user.email "awesome.developer.@example.org"

If you have authored a commit that is missing the signed-off-by line, you can amend your
commits and push them to GitHub.

.. code-block:: bash

   git commit --amend --signoff

If you've pushed your changes to GitHub already you'll need to force push your branch
after this with ``git push -f``.

Commit Message formatting
-------------------------

We request that your first commit follow a particular format, and we
**require** that your PR title follow the format. The format is:

.. code-block:: bash

    FEAT-#9999: Add some functionality to enable something

The ``FEAT`` component represents the type of commit. This component of the commit
message can be one of the following:

* FEAT: A new feature that is added
* DOCS: Documentation improvements or updates
* FIX: A bugfix contribution
* REFACTOR: Moving or removing code without change in functionality
* TEST: Test updates or improvements
* PERF: Performance enhancements

The ``#9999`` component of the commit message should be the issue number in the unidist
GitHub issue tracker: https://github.com/modin-project/unidist/issues. This is important
because it links commits to their issues.

The commit message should follow a colon (:) and be descriptive and succinct.

A unidist CI job on GitHub will enforce that your pull request title follows the
format we suggest. Note that if you update the PR title, you have to push
another commit (even if it's empty) or amend your last commit for the job to
pick up the new PR title. Re-running the job in Github Actions won't work.

General Rules for Committers
----------------------------

- Try to write a PR name as descriptive as possible.
- Try to keep PRs as small as possible. One PR should be making one semantically atomic change.
- Don't merge your own PRs even if you are technically able to do it.

Development Dependencies
------------------------

We recommend doing development in a virtualenv or conda environment, though this decision
is ultimately yours. You will want to run the following in order to install all of the required
dependencies for running the tests and formatting the code:

.. code-block:: bash

  conda env create --file environment_linux.yml # for Linux
  conda env create --file environment_win.yml # for Windows
  # or
  pip install -r requirements.txt

Code Formatting and Lint
------------------------

We use black_ for code formatting. Before you submit a pull request, please make sure
that you run the following from the project root:

.. code-block:: bash

  black .

We also use flake8_ to check linting errors. Running the following from the project root
will ensure that it passes the lint checks on Github Actions:

.. code-block:: bash

  flake8 .

We test that this has been run on our `Github Actions`_ test suite. If you do this and find
that the tests are still failing, try updating your version of black and flake8.

Adding a test
-------------

If you find yourself fixing a bug or adding a new feature, don't forget to add a test to
the test suite to verify its correctness! We ask that you follow the existing
structure of the tests for ease of maintenance.

Running the tests
-----------------

To run the entire test suite, run the following from the project root:

.. code-block:: bash

  python -m pytest unidist/test

If you've only modified a small amount of code, it may be sufficient to run a single test or
some subset of the test suite. In order to run a specific test run:

.. code-block:: bash

  python -m pytest unidist/test/test_new_functionality.py::test_new_functionality

The entire test suite is automatically run for each pull request.

Building documentation
----------------------

To build the documentation, please follow the steps below from the project root (it is supposed you have
dependencies from ``environment_linux.yml``(``environment_win.yml``) or ``requirements.txt`` installed):

.. code-block:: bash

    # Build unidist to make C++ extensions available and also
    # for correct module imports when building the documentation.
    pip install -e .
    cd docs
    sphinx-build -b html . build

To visualize the documentation locally, run the following from `build` folder:

.. code-block:: bash

    python -m http.server <port>
    # python -m http.server 1234

then open the browser at `0.0.0.0:<port>` (e.g. `0.0.0.0:1234`).

.. _black: https://black.readthedocs.io/en/latest
.. _flake8: http://flake8.pycqa.org/en/latest
.. _Github Actions: https://github.com/features/actions
