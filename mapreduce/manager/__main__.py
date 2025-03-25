"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import shutil
from pathlib import Path
import click
import threading
import queue
from mapreduce.utils import tcp_server
from mapreduce.utils import udp_server
from mapreduce.utils import ThreadSafeOrderedDict
from mapreduce.utils import RemoteWorker
from mapreduce.utils import Job


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # Manager attributes
        self.host = host
        self.port = port
        self.threads = []
        self.job_id = 0
        self.job_status = 0 # Is job completed?
        self.jobs = queue.Queue()
        self.workers = ThreadSafeOrderedDict() 
        self.signals = ThreadSafeOrderedDict()
        self.signals["shutdown"] = False

        # TCP thread for messages
        messages_thread = threading.Thread(target=tcp_server,args=(self.host,
                                                self.port,
                                                self.signals,
                                                self.handle_messages,))
        self.threads.append(messages_thread)
        messages_thread.start()

        # UDP thread for heartbeats
        worker_thread = threading.Thread(target=udp_server,args=(self.host,
                                                self.port,
                                                self.signals,
                                                self.handle_heartbeats))
        self.threads.append(worker_thread)
        worker_thread.start()

        # Thread for checking worker statuses
        workers_status_thread = threading.Thread(target=self.check_heartbeats)
        workers_status_thread.start()

        # TODO: not sure if this is needed
        # Fault tolerance thread
        # fault_tolerance_thread = threading.Thread(target=self.fault_tolerance, daemon=True)
        # self.threads.append(fault_tolerance_thread)
        # fault_tolerance_thread.start()

        self.handle_jobs()
        # Join threads when they stop running
        for thread in self.threads:
            thread.join()

    def handle_heartbeats(self, worker):
            """Update worker heartbeat."""
            if worker["message_type"] == "heartbeat" and len(self.workers) > 0:
                # Get worker's host and port
                host = worker["worker_host"]
                port = worker["worker_port"]
                if (host, port) in self.workers:
                    # If worker is alive, update heartbeat
                    if self.workers[(host, port)].status != "dead":
                        self.workers[(host, port)].update_heartbeat(time.time())

    def check_heartbeats(self):
            """Check if any workers are dead."""
            while not self.signals["shutdown"]:
                for _, val in self.workers.items():
                    # Wait 10 secs
                    if val.check_if_missing(time.time()):
                        if val.status != "dead":
                            # Worker is dead
                            LOGGER.info("marked worker as dead %s ", (
                                val.host, val.port))
                            val.update_status("dead")
                            val = val.unassign_task()
                            if val is not None:
                                val[1].task_reset(val[0])
                time.sleep(1)

    def assign_task(self, job, directory, worker_num, task):
            """Assign task to worker."""
            if job.state == "mapping":
                self.workers[worker_num].assign_mapper(task.file_paths,
                                                    task.task_id,
                                                    directory,
                                                    job,
                                                    task)
                LOGGER.info("assigned MAPPER")
            else:
                self.workers[worker_num].assign_reducer(task.file_paths,
                                                        task.task_id,
                                                        directory,
                                                        job,
                                                        task)
                LOGGER.info("assigned REDUCER")

    def get_worker(self, job, tmpdir):
            """Get next ready worker."""
            worker_num = None
            for key, val in self.workers.items():
                if val.status == "ready":
                    worker_num = key
                    LOGGER.info("picked worker %s", key)
                    break
            # If there is an available workers
            if worker_num:
                self.workers[worker_num].update_status("busy")
                if job.state == "mapping":
                    directory = tmpdir
                else:
                    directory = str(job.out_dir)
                task = job.task_next()
                self.assign_task(job, directory, worker_num, task)


    def handle_jobs(self):
            """Assign jobs to workers and run."""
            while not self.signals["shutdown"]:
                # While not shutdown and there are more jobs
                if self.jobs.qsize() > 0:
                    # Get job
                    job = self.jobs.get()
                    job.next_state()
                    # Delete directory if it already exists
                    if job.out_dir.exists() and job.out_dir.is_dir():
                        shutil.rmtree(job.out_dir)
                    job.out_dir.mkdir(parents=True, exist_ok=True)
                    prefix = f"mapreduce-shared-job{job.id_:05d}-"
                    self.job_status = 0
                    info = job.info["num_mappers"]

                    # Create shared directory
                    with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                        LOGGER.info("Created tmpdir %s", tmpdir)
                        while not self.signals["shutdown"] and job.state != "f":
                            if len(self.workers) > 0:
                                if self.job_status == info and not job.task_ready():
                                    # Assign job
                                    job.next_state(Path(tmpdir))
                                    self.job_status = 0
                                    info = 0
                                    if job.state == "reducing":
                                        LOGGER.info("starting REDUCER")
                                        info = job.info["num_reducers"]
                                elif job.task_ready():
                                    self.get_worker(job, tmpdir)
                    LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def handle_messages(self, message_dict):
            """Handle messages."""
            message = message_dict["message_type"]
            new_message = {}

            # Shut down
            if message == "shutdown":
                self.signals["shutdown"] = True
                new_message = message_dict
                for _, val in self.workers.items():
                    if val.status != "dead":
                        val.send_message(new_message)

            # Register new worker
            elif message == "register":
                new_message = {"message_type": "register_ack"}
                worker = RemoteWorker(
                    message_dict["worker_host"],
                    message_dict["worker_port"])
                w = (message_dict["worker_host"], message_dict["worker_port"])
                if w in self.workers and self.workers[w].task:
                    val = self.workers[w].unassign_task()
                    if val is not None:
                        val[1].task_reset(val[0])
                self.workers[w] = worker
                worker.send_message(new_message)

            # Add new job
            elif message == "new_manager_job":
                job = Job(self.job_id,
                        message_dict["input_directory"],
                        message_dict["output_directory"],
                        message_dict)
                self.job_id += 1
                self.jobs.put(job)

            # Job is done
            elif message == "finished":
                self.job_status += 1
                w = (message_dict["worker_host"], message_dict["worker_port"])
                self.workers[w].update_status("ready")
                LOGGER.info("worker %s is finished and marked ready", w)
                LOGGER.info("CURR counter is %s", self.job_status)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
