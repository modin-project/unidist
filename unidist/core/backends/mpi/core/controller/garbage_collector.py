# Copyright (C) 2021-2023 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`GarbageCollector` functionality."""

import time

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.async_operations import AsyncOperations
from unidist.core.backends.mpi.core.serialization import SimpleDataSerializer
from unidist.core.backends.mpi.core.controller.object_store import object_store


logger = common.get_logger("utils", "utils.log")


class GarbageCollector:
    """
    Class that tracks deleted data IDs and cleans worker's object storage.

    Parameters
    ----------
    object_store : unidist.core.backends.mpi.executor.ObjectStore
        Reference to the local object storage.

    Notes
    -----
    Cleanup relies on internal threshold settings.
    """

    def __init__(self, object_store):
        # Cleanup frequency settings
        self._cleanup_counter = 1
        self._cleanup_threshold = 5
        self._time_threshold = 1  # seconds
        self._timestamp = 0  # seconds
        # Cleanup list of DataIDs
        self._cleanup_list = []
        self._cleanup_list_threshold = 10
        # Reference to the global object store
        self._object_store = object_store
        # Task submitted counter
        self._task_counter = 0

    def _send_cleanup_request(self, cleanup_list):
        """
        Send a list of data IDs to be deleted for each worker to cleanup local storages.

        Parameters
        ----------
        cleanup_list : list
            List of data IDs.
        """
        logger.debug(
            "Send cleanup list - {}".format(
                common.unwrapped_data_ids_list(cleanup_list)
            )
        )
        mpi_state = communication.MPIState.get_instance()
        # Cache serialized list of data IDs
        s_cleanup_list = SimpleDataSerializer().serialize_pickle(cleanup_list)
        async_operations = AsyncOperations.get_instance()
        for rank_id in mpi_state.workers:
            if rank_id != mpi_state.rank:
                h_list = communication.isend_serialized_operation(
                    mpi_state.comm,
                    common.Operation.CLEANUP,
                    s_cleanup_list,
                    rank_id,
                )
                async_operations.extend(h_list)

    def increment_task_counter(self):
        """
        Track task submission number.

        Notes
        -----
        For cleanup purpose.
        """
        self._task_counter += 1

    def collect(self, data_id):
        """
        Track ID destruction.

        This ID is out of scope, it's associated data should be cleared.

        Parameters
        ----------
        data_id : unidist.core.backends.mpi.core.common.MasterDataID
            An ID to data
        """
        self._cleanup_list.append(data_id)

    def regular_cleanup(self):
        """
        Cleanup all garbage collected IDs from local and all workers object storages.

        Cleanup triggers based on internal threshold settings.
        """
        logger.debug("Cleanup list len - {}".format(len(self._cleanup_list)))
        logger.debug(
            "Cleanup counter {}, threshold reached - {}".format(
                self._cleanup_counter,
                (self._cleanup_counter % self._cleanup_threshold) == 0,
            )
        )
        async_operations = AsyncOperations.get_instance()
        # Check completion status of previous async MPI routines
        async_operations.check()
        if len(self._cleanup_list) > self._cleanup_list_threshold:
            if self._cleanup_counter % self._cleanup_threshold == 0:
                timestamp_snapshot = time.perf_counter()
                if (timestamp_snapshot - self._timestamp) > self._time_threshold:
                    logger.debug("Cleanup counter {}".format(self._cleanup_counter))

                    mpi_state = communication.MPIState.get_instance()
                    root_monitor = mpi_state.get_monitor_by_worker_rank(
                        communication.MPIRank.ROOT
                    )
                    # Compare submitted and executed tasks
                    # We use a blocking send here because we have to wait for
                    # completion of the communication, which is necessary for the pipeline to continue.
                    communication.mpi_send_operation(
                        mpi_state.comm,
                        common.Operation.GET_TASK_COUNT,
                        root_monitor,
                    )
                    executed_task_counter = communication.mpi_recv_object(
                        mpi_state.comm,
                        root_monitor,
                    )

                    logger.debug(
                        "Submitted task count {} vs executed task count {}".format(
                            self._task_counter, executed_task_counter
                        )
                    )
                    if executed_task_counter == self._task_counter:
                        self._send_cleanup_request(self._cleanup_list)
                        # Clear the remaining references
                        self._object_store.clear(self._cleanup_list)
                        self._cleanup_list.clear()
                        self._cleanup_counter += 1
                        self._timestamp = time.perf_counter()
            else:
                self._cleanup_counter += 1


garbage_collector = GarbageCollector(object_store)
