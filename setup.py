import pathlib
from setuptools import setup, find_packages
import sys

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

ray_deps = ["ray[default]"]
dask_deps = ["dask[complete]>=2.22.0", "distributed>=2.22.0"]
mpi_deps = ["mpi4py", "pandas", "msgpack"]
if sys.version_info[1] < 8:
    mpi_deps += "pickle5"
all_deps = ray_deps + dask_deps + mpi_deps

setup(
    name="unidist",
    version="0.0.1",
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
)
