<p align="center">
    <a href="https://unidist.readthedocs.io"><img alt="" src="https://github.com/modin-project/unidist/blob/d17f0da551846277c9d56a7f5e7d8f244c09d660/docs/img/unidist-logo-simple-628x128.png?raw=true"></a>
</p>
<h2 align="center">Unified Distributed Execution</h2>

<p align="center">
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

Also, the framework provides a sequential ``Python`` backend, that can be used for debugging.

### Installation

Currently, unidist can be installed from sources using ``pip``:

```bash
# Dependencies for `MultiProcessing` and `Python` backends will be installed as these are supported by default
$ pip install git+https://github.com/modin-project/unidist
# Dependencies for all the execution backends will be installed
$ pip install git+https://github.com/modin-project/unidist#egg=unidist[all]
# Dependencies for `Ray` execution backend will be installed
$ pip install git+https://github.com/modin-project/unidist#egg=unidist[ray]
```

This will install unidist directly from the repo without you having to manually clone it! Please be aware that the latest changes have not made it into a release and may not be completely stable.

#### Choosing an execution backend

There are several ways to choose the execution backend for distributed computation.
First, the recommended way is to use
[unidist CLI](https://unidist.readthedocs.io/en/latest/using_cli.html) options:

```bash
# Running the script with unidist on Ray backend
$ unidist script.py --backend ray
# Running the script with unidist on Dask backend
$ unidist script.py --backend dask
```

Second, setting the environment variable:

```bash
# unidist will use Ray backend to distribute computations
export UNIDIST_BACKEND=ray
# unidist will use Dask backend to distribute computations
export UNIDIST_BACKEND=dask
```

Third, using [config API](https://unidist.readthedocs.io/en/latest/flow/unidist/config.html) directly in your script:

```python
import unidist.config as cfg
cfg.Backend.put("ray") # unidist will use Ray backend to distribute computations
import unidist.config as cfg
cfg.Backend.put("dask") # unidist will use Dask backend to distribute computations
```

Default execution backend for unidist is Ray.

#### Usage

unidist provides [CLI interface](https://unidist.readthedocs.io/en/latest/using_cli.html) to run python programs.

```python
import unidist
unidist.init() # Ray backend is used by default

@unidist.remote
def foo(x):
    return x * x

# This will run `foo` on a pool of workers in parallel;
# `refs` will contain object references to actual data
refs = [foo.remote(i) for i in range(5)]
# To get the data call `unidist.get(...)`
print(unidist.get(refs))
[0, 1, 4, 9, 16]
```

For more examples refer to [Getting Started](https://unidist.readthedocs.io/en/latest/getting_started.html) section
in our documentation.

### Full Documentation

Visit the complete documentation on readthedocs: https://unidist.readthedocs.io.
