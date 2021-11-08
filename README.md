# Unified Distributed Execution

The framework supports multiple execution backends: Ray, Dask, MPI and MultiProcessing.

To run tests you need to install ``pytest`` package:

```
pip install pytest
```
Run tests using command:
```
pytest unidist/test/
```

For unidist MPI backend follow additional [instructions](unidist/core/backends/mpi/README.md).
