"""Job class."""
import os
from pathlib import Path
import queue
import logging
from mapreduce.utils.task import Task

LOGGER = logging.getLogger(__name__)


class Job:
    """Job class."""

    def __init__(self, id_, input_dir, out_dir, info):
        """Job constructor."""
        self.id_ = id_
        self.input_dir = Path(input_dir)
        self.out_dir = Path(out_dir)
        self.info = info
        self.tasks = queue.Queue()
        self.state = "start"

    def next_state(self, new_dir=None):
        """Change job state."""
        if self.state == "start":
            self.state = "mapping"
            self.mapping_partition()
        elif self.state == "mapping":
            self.state = "reducing"
            self.reducing_partition(new_dir)
        else:
            self.state = "f"

    def mapping_partition(self):
        """Partition mapping."""
        LOGGER.info("STARTING JOB MAPPING PARTITION")
        scan = [[] for _ in range(self.info["num_mappers"])]
        all_entries = os.listdir(self.input_dir)
        files = [str(self.input_dir/entry) for entry in all_entries
                 if os.path.isfile(os.path.join(self.input_dir, entry))]
        files.sort()
        for i, filename in enumerate(files):
            index = i % self.info["num_mappers"]
            scan[index].append(filename)
        num_files = len(scan)
        for i in range(num_files):
            self.tasks.put(Task(i, "map", scan[i]))

    def reducing_partition(self, new_dir):
        """Partition reducing."""
        LOGGER.info("STARTING JOB REDUCING PARTITION")
        scan = [[] for _ in range(self.info["num_reducers"])]
        for i in range(self.info["num_reducers"]):
            pattern = f"maptask*-part{str(i).zfill(5)}"
            str_files = sorted(str(f) for f in new_dir.glob(pattern))
            for s in str_files:
                scan[i].append(s)
        num_files = len(scan)
        for i in range(num_files):
            self.tasks.put(Task(i, "reduce", scan[i]))

    def task_reset(self, task):
        """Put task back into queue to be reassigned."""
        self.tasks.put(task)

    def task_ready(self):
        """Return if next task is ready."""
        return self.tasks.qsize() > 0

    def task_next(self):
        """Return next task."""
        return self.tasks.get()
