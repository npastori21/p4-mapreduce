"""Remote Worker class."""
import time
import logging
from mapreduce.utils.network import tcp_client
from mapreduce.utils.ordered_dict import ThreadSafeOrderedDict

TIME_LIMIT = 10
LOGGER = logging.getLogger(__name__)


class RemoteWorker:
    """Manager's view of a worker."""

    def __init__(self, host, port):
        """Remote worker constructor."""
        self.host = host
        self.port = port
        self.status = "ready"
        self.last_heartbeat = time.time()
        self.task = None

    def assign_mapper(self, task, task_id, directory, job, task_obj):
        """Assign a mapping task to this Worker."""
        m = ThreadSafeOrderedDict({
                "message_type": "new_map_task",
                "task_id": task_id,
                "input_paths": task,
                "executable": job.info["mapper_executable"],
                "output_directory": directory,
                "num_partitions": job.info["num_reducers"],
            })
        self.task = (task_obj, job)
        self.send_message(m)

    def assign_reducer(self, task, task_id, directory, job, task_obj):
        """Assign a reduce task to this Worker."""
        m = ThreadSafeOrderedDict({
                "message_type": "new_reduce_task",
                "task_id": task_id,
                "input_paths": task,
                "executable": job.info["reducer_executable"],
                "output_directory": directory
            })
        self.task = (task_obj, job)
        self.send_message(m)

    def send_message(self, message):
        """Send message."""
        try:
            tcp_client(message, self.host, self.port)
        except ConnectionRefusedError:
            LOGGER.info("CONNECTION REFUSED: worker %s marked dead",
                        (self.host, self.port))
            self.status = "dead"
            if self.current_task is not None:
                task, job = self.task
                job.task_reset(task)
                self.task = None

    def update_heartbeat(self, time_):
        """Update last heartbeat."""
        self.last_heartbeat = time_

    def check_if_missing(self, time_) -> bool:
        """Check if worker has missed too many pings."""
        if time_ - self.last_heartbeat > TIME_LIMIT:
            return True
        return False

    def unassign_task(self):
        """Unassign task when worker is dead."""
        if self.task:
            old_task, old_job = self.task
            self.task = None
            return old_task, old_job
        return None

    def update_status(self, status):
        """Update status of worker."""
        self.status = status

   
