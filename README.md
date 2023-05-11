<p align="center">
    <a href="https://unidist.readthedocs.io"><img alt="" src="https://github.com/modin-project/unidist/blob/d17f0da551846277c9d56a7f5e7d8f244c09d660/docs/img/unidist-logo-simple-628x128.png?raw=true"></a>
</p>
<h2 align="center">Unified Distributed Execution</h2>

<p align="center">
<a href="https://github.com/modin-project/unidist/actions"><img src="https://github.com/modin-project/unidist/workflows/master/badge.svg" align="center"></a>
<a href="https://unidist.readthedocs.io/en/latest/?badge=latest"><img alt="" src="https://readthedocs.org/projects/unidist/badge/?version=latest" align="center"></a>
<a href="https://pypi.org/project/unidist/"><img src="https://badge.fury.io/py/unidist.svg" alt="PyPI version" align="center"></a>
</p>

### What is unidist?

unidist is a framework that is intended to provide the unified API for distributed execution by supporting various performant execution backends. At the moment the following backends are supported under the hood:

* [Ray](https://docs.ray.io/en/master/index.html)
* [MPI](https://www.mpi-forum.org/)
* [Dask Distributed](https://distributed.dask.org/en/latest/)
* [Python Multiprocessing](https://docs.python.org/3/library/multiprocessing.html)

unidist is designed to work in a [task-based parallel](https://en.wikipedia.org/wiki/Task_parallelism) model.

Also, the framework provides a Python Sequential backend (`pyseq`), that can be used for debugging.

### Installation

#### Using pip

unidist can be installed with `pip` on Linux, Windows and MacOS:

```bash
pip install unidist # Install unidist with dependencies for Python Multiprocessing and Python Sequential backends
```

unidist can also be used with Dask, MPI or Ray execution backend.
If you don't have Dask, MPI or Ray installed, you will need to install unidist with one of the targets:

```bash
pip install unidist[all] # Install unidist with dependencies for all the backends
pip install unidist[dask] # Install unidist with dependencies for Dask backend
pip install unidist[mpi] # Install unidist with dependencies for MPI backend
pip install unidist[ray] # Install unidist with dependencies for Ray backend
```

unidist automatically detects which execution backends are installed and uses that for scheduling computation.

**Note:** There are different MPI implementations, each of which can be used as a backend in unidist.
By default, mapping `unidist[mpi]` installs MPICH on Linux and MacOS and MSMPI on Windows. If you want to use
a specific version of MPI, you can install the core dependencies of unidist as `pip install unidist` and then
install the specific version of MPI using pip as shown in the [installation](https://mpi4py.readthedocs.io/en/latest/install.html)
section of mpi4py documentation.

#### Using conda

For installing unidist with dependencies for Dask and MPI execution backends into a conda environment
the following command should be used:

```bash
conda install unidist-dask unidist-mpi -c conda-forge
```

All set of backends could be available in a conda environment by specifying:

```bash
conda install unidist-all -c conda-forge
```

or explicitly:

```bash
conda install unidist-dask unidist-mpi unidist-ray -c conda-forge
```

**Note:** There are different MPI implementations, each of which can be used as a backend in unidist.
By default, mapping `unidist-mpi` installs MPICH on Linux and MacOS and MSMPI on Windows. If you want to use
a specific version of MPI, you can install the core dependencies of unidist as `conda install unidist` and then
install the specific version of MPI using conda as shown in the [installation](https://mpi4py.readthedocs.io/en/latest/install.html)
section of mpi4py documentation. That said, it is highly encouraged to use your own MPI binaries as stated in the
[Using External MPI Libraries](https://conda-forge.org/docs/user/tipsandtricks.html#using-external-message-passing-interface-mpi-libraries)
section of the conda-forge documentation in order to get ultimate performance.

For more information refer to [Installation](https://unidist.readthedocs.io/en/latest/installation.html) section.

#### Choosing an execution backend

If you want to choose a specific execution backend to run on,
you can set the environment variable `UNIDIST_BACKEND` and unidist will do computation with that backend:

```bash
export UNIDIST_BACKEND=ray  # unidist will use Ray
export UNIDIST_BACKEND=mpi  # unidist will use MPI
export UNIDIST_BACKEND=dask  # unidist will use Dask
```

This can also be done within a notebook/interpreter before you initialize unidist:

```python
from unidist.config import Backend

Backend.put("ray")  # unidist will use Ray
Backend.put("mpi")  # unidist will use MPI
Backend.put("dask")  # unidist will use Dask
```

If you have installed all the execution backends and haven't specified any of the execution backends, MPI is used by default.

Since some of the execution backends, particularly, MPI, have some specifics regarding running python programs, please
refer to [Using Unidist](https://unidist.readthedocs.io/en/latest/using_unidist/index.html) section to get more information on
setting the execution backend to run on.

#### Usage

```python
# script.py

import unidist
unidist.init() # MPI backend is used by default

@unidist.remote
def foo(x):
    return x * x

# This will run `foo` on a pool of workers in parallel;
# `refs` will contain object references to actual data
refs = [foo.remote(i) for i in range(5)]
# To get the data call `unidist.get(...)`
print(unidist.get(refs))
```

Run the `script.py` with:

```bash
$ python script.py
[0, 1, 4, 9, 16] # output
```

For more examples refer to [Getting Started](https://unidist.readthedocs.io/en/latest/getting_started.html) section
in our documentation.

### Powered by unidist

unidist is meant to be used not only directly by users to get better performance in their workloads,
but also be a core component of other libraries to power those with the performant execution backends.
Refer to `Libraries powered by unidist` section of [Using Unidist](https://unidist.readthedocs.io/en/latest/using_unidist/index.html) page
to get more information on which libraries have already been using unidist.

### Full Documentation

Visit the complete documentation on readthedocs: https://unidist.readthedocs.io.
