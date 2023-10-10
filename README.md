<p align="center">
    <a href="https://unidist.readthedocs.io"><img alt="" src="https://github.com/modin-project/unidist/blob/d17f0da551846277c9d56a7f5e7d8f244c09d660/docs/img/unidist-logo-simple-628x128.png?raw=true"></a>
</p>
<h2 align="center">Unified Distributed Execution</h2>

<p align="center">
<a href="https://github.com/modin-project/unidist/actions"><img src="https://github.com/modin-project/unidist/actions/workflows/ci.yml/badge.svg" align="center"></a>
<a href="https://unidist.readthedocs.io/en/latest/?badge=latest"><img alt="" src="https://readthedocs.org/projects/unidist/badge/?version=latest" align="center"></a>
<a href="https://pypi.org/project/unidist/"><img src="https://badge.fury.io/py/unidist.svg" alt="PyPI version" align="center"></a>
<a href="https://pepy.tech/project/unidist"><img src="https://static.pepy.tech/personalized-badge/unidist?period=total&units=international_system&left_color=black&right_color=blue&left_text=Downloads" align="center"></a>
</p>

### What is unidist?

unidist is a framework that is intended to provide the unified API for distributed execution by supporting various performant execution backends. At the moment the following backends are supported under the hood:

* [MPI](https://www.mpi-forum.org/)
* [Dask Distributed](https://distributed.dask.org/en/latest/)
* [Ray](https://docs.ray.io/en/master/index.html)
* [Python Multiprocessing](https://docs.python.org/3/library/multiprocessing.html)

unidist is designed to work in a [task-based parallel](https://en.wikipedia.org/wiki/Task_parallelism) model.

Also, the framework provides a Python Sequential backend (`pyseq`), that can be used for debugging.

### Installation

#### Using pip

unidist can be installed with `pip` on Linux, Windows and MacOS:

```bash
pip install unidist # Install unidist with dependencies for Python Multiprocessing and Python Sequential backends
```

unidist can also be used with MPI, Dask or Ray execution backend.
If you don't have MPI, Dask or Ray installed, you will need to install unidist with one of the targets:

```bash
pip install unidist[all] # Install unidist with dependencies for all the backends
pip install unidist[mpi] # Install unidist with dependencies for MPI backend
pip install unidist[dask] # Install unidist with dependencies for Dask backend
pip install unidist[ray] # Install unidist with dependencies for Ray backend
```

unidist automatically detects which execution backends are installed and uses that for scheduling computation.

**Note:** There are different MPI implementations, each of which can be used as a backend in unidist.
Mapping `unidist[mpi]` installs `mpi4py` package, which is just a Python wrapper for MPI.
To enable unidist on MPI execution you need to have a working MPI implementation and certain software installed beforehand.
Refer to [Installation](https://mpi4py.readthedocs.io/en/latest/install.html) page of the `mpi4py` documentation for details.
Also, you can find some instructions on [MPI backend](https://unidist.readthedocs.io/en/latest/optimization_notes/mpi.html) page.

#### Using conda

For installing unidist with dependencies for MPI and Dask execution backends into a conda environment
the following command should be used:

```bash
conda install unidist-mpi unidist-dask -c conda-forge
```

All set of backends could be available in a conda environment by specifying:

```bash
conda install unidist-all -c conda-forge
```

or explicitly:

```bash
conda install unidist-mpi unidist-dask unidist-ray -c conda-forge
```

**Note:** There are different MPI implementations, each of which can be used as a backend in unidist.
By default, mapping `unidist-mpi` installs a default MPI implementation, which comes with `mpi4py` package and is ready to use.
The conda dependency solver decides on which MPI implementation is to be installed. If you want to use a specific version of MPI,
you can install the core dependencies for MPI backend and the specific version of MPI as `conda install unidist-mpi <mpi>`
as shown in the [Installation](https://mpi4py.readthedocs.io/en/latest/install.html)
page of `mpi4py` documentation. That said, it is highly encouraged to use your own MPI binaries as stated in the
[Using External MPI Libraries](https://conda-forge.org/docs/user/tipsandtricks.html#using-external-message-passing-interface-mpi-libraries)
section of the conda-forge documentation in order to get ultimate performance.

For more information refer to [Installation](https://unidist.readthedocs.io/en/latest/installation.html) section.

#### Choosing an execution backend

If you want to choose a specific execution backend to run on,
you can set the environment variable `UNIDIST_BACKEND` and unidist will do computation with that backend:

```bash
export UNIDIST_BACKEND=mpi  # unidist will use MPI
export UNIDIST_BACKEND=dask  # unidist will use Dask
export UNIDIST_BACKEND=ray  # unidist will use Ray
```

This can also be done within a notebook/interpreter before you initialize unidist:

```python
from unidist.config import Backend

Backend.put("mpi")  # unidist will use MPI
Backend.put("dask")  # unidist will use Dask
Backend.put("ray")  # unidist will use Ray
```

If you have installed all the execution backends and haven't specified any of the execution backends, MPI is used by default.
Currently, almost all MPI implementations require ``mpiexec`` command to be used when running an MPI program.
If you use a backend other than MPI, you run a program as a regular python script (see below).

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
$ mpiexec -n 1 python script.py  # for MPI backend
# $ python script.py  # for any other supported backend
[0, 1, 4, 9, 16]  # output
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
