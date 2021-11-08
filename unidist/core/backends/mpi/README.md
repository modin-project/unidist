# Unidist MPI Backend

In order to use unidist MPI backend you have to set the following environment variable:

```bash
export UNIDIST_BACKEND=MPI
```

To start an application with the backend, use `mpirun` command on the main script `APP_SCRIPT`.
Additionally, run monitoring process and `NUM_WORKERS` MPI worker processes for tasks execution.
The command accepts machine addresses using the `host` option where each process is run.

```bash
mpirun -host localhost -n 1 python $APP_SCRIPT \
    : -n 1 -wdir /tmp/mpi-py python unidist/core/backends/mpi/core/monitor.py \
    : -n $NUM_WORKERS -wdir /tmp/mpi-py python unidist/core/backends/mpi/core/worker.py
