import pathlib
from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize
import sys
import versioneer

# https://github.com/modin-project/unidist/issues/324
ray_deps = ["ray[default]>=1.13.0", "pydantic<2"]
dask_deps = ["dask[complete]>=2.22.0", "distributed>=2.22.0"]
mpi_deps = ["mpi4py-mpich", "msgpack>=1.0.0"]
if sys.version_info[1] < 8:
    mpi_deps += "pickle5"
all_deps = ray_deps + dask_deps + mpi_deps

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

_memory = Extension(
    "unidist.core.backends.mpi.core._memory",
    ["unidist/core/backends/mpi/core/memory/_memory.pyx"],
    language="c++",
)

setup(
    name="unidist",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Unified Distributed Execution",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    packages=find_packages(),
    url="https://github.com/modin-project/unidist",
    install_requires=["packaging", "cloudpickle"],
    extras_require={
        # can be installed by pip install unidist[ray]
        "ray": ray_deps,
        "dask": dask_deps,
        "mpi": mpi_deps,
        "all": all_deps,
    },
    python_requires=">=3.7.1",
    ext_modules=cythonize([_memory]),
)
