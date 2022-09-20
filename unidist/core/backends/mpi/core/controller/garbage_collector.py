# Copyright (C) 2021-2022 Modin authors
#
# SPDX-License-Identifier: Apache-2.0

"""`GarbageCollector` functionality."""

import time

import unidist.core.backends.mpi.core.common as common
import unidist.core.backends.mpi.core.communication as communication
from unidist.core.backends.mpi.core.serialization import SimpleDataSerializer
from unidist.core.backends.mpi.core.controller.object_store import object_store
from unidist.core.backends.mpi.core.controller.common import initial_worker_number


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
        for rank_id in range(initial_worker_number, mpi_state.world_size):
            communication.send_serialized_operation(
                mpi_state.comm,
                common.Operation.CLEANUP,
                s_cleanup_list,
                rank_id,
            )

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
        if len(self._cleanup_list) > self._cleanup_list_threshold:
            if self._cleanup_counter % self._cleanup_threshold == 0:

                timestamp_snapshot = time.perf_counter()
                if (timestamp_snapshot - self._timestamp) > self._time_threshold:
                    logger.debug("Cleanup counter {}".format(self._cleanup_counter))

                    mpi_state = communication.MPIState.get_instance()
                    # Compare submitted and executed tasks
                    communication.mpi_send_object(
                        mpi_state.comm,
                        common.Operation.GET_TASK_COUNT,
                        communication.MPIRank.MONITOR,
                    )
                    executed_task_counter = communication.recv_simple_operation(
                        mpi_state.comm,
                        communication.MPIRank.MONITOR,
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
