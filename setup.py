import os
import pathlib
from setuptools import setup, find_packages
from setuptools.dist import Distribution
import sys
import versioneer

try:
    from wheel.bdist_wheel import bdist_wheel

    HAS_WHEEL = True
except ImportError:
    HAS_WHEEL = False

if HAS_WHEEL:

    class UnidistWheel(bdist_wheel):
        def finalize_options(self):
            bdist_wheel.finalize_options(self)
            self.root_is_pure = False

        def get_tag(self):
            _, _, plat = bdist_wheel.get_tag(self)
            py = "py3"
            abi = "none"
            return py, abi, plat


class UnidistDistribution(Distribution):
    def __init__(self, *attrs):
        Distribution.__init__(self, *attrs)
        if HAS_WHEEL:
            self.cmdclass["bdist_wheel"] = UnidistWheel

    def is_pure(self):
        return False


ray_deps = ["ray[default]"]
dask_deps = ["dask[complete]>=2.22.0", "distributed>=2.22.0"]
mpi_deps = ["mpi4py-mpich", "msgpack"]

if "SETUP_PLAT_NAME" in os.environ:
    if "macos" in os.environ["SETUP_PLAT_NAME"]:
        all_deps = ray_deps + dask_deps
    else:
        all_deps = ray_deps + dask_deps + mpi_deps
else:
    all_deps = (
        ray_deps + dask_deps
        if sys.platform == "darwin"
        else dask_deps + ray_deps + mpi_deps
    )

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="unidist",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    distclass=UnidistDistribution,
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
    entry_points={"console_scripts": ["unidist = unidist.cli.__main__:main"]},
    python_requires=">=3.8",
)
