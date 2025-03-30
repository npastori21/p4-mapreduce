"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import time
import shutil
from pathlib import Path
import queue
import threading
import click

from mapreduce.utils import tcp_server as tcp
from mapreduce.utils import udp_server as udp
from mapreduce.utils import ThreadSafeOrderedDict
from mapreduce.utils import RemoteWorker
from mapreduce.utils import Job


# Configure logging
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, filename="debug.log")


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
        self.job_id = 0
        self.job_status = 0  # Is job completed?
        self.jobs = queue.Queue()
        self.workers = ThreadSafeOrderedDict()
        self.signals = ThreadSafeOrderedDict()
        self.signals["shutdown"] = False
        LOGGER.info("MHost %s, port %s", self.host, self.port)
        with tempfile.TemporaryDirectory(prefix='mapreduce-shared-') as tmpdir:
            # UDP thread for heartbeats (port =+ 1 to prevent overlap)
            worker_thread = threading.Thread(target=udp, args=(self.host,
                                                               self.port,
                                                               self.signals,
                                                               self.h_beats))
            worker_thread.start()

            # TCP thread for messages
            messages_thread = threading.Thread(target=tcp, args=(self.host,
                                                                 self.port,
                                                                 self.signals,
                                                                 self.msgs))
            messages_thread.start()

            # Thread for checking worker statuses
            status_thread = threading.Thread(target=self.check_heartbeats)
            status_thread.start()

            LOGGER.info("Starting jobs")
            self.handle_jobs(tmpdir)
            LOGGER.info("JOBS COMPLETED")

            # Join threads when they stop running
            worker_thread.join()
            LOGGER.info("Workers thread joined")
            messages_thread.join()
            LOGGER.info("Messages thread joined")
            status_thread.join()
            LOGGER.info("Status thread joined")

            LOGGER.info("Done")

    def h_beats(self, message):
        """Update worker heartbeat."""
        LOGGER.info("Received heartbeat message: %s", message)
        if message["message_type"] == "heartbeat" and len(self.workers) > 0:
            LOGGER.info("MANAGER RECEIVED HEARTBEAT")
            # Get worker's host and port
            host = message["worker_host"]
            port = message["worker_port"]
            if (host, port) in self.workers:
                # If worker is alive, update heartbeat
                
                if self.workers[(host, port)].status != "dead":
                    self.workers[(host, port)].update_heartbeat(time.time())
                    LOGGER.info("Updated %s's heartbeat", (host, port))
                else:
                    LOGGER.info(" %s is dead", (host, port))
            else:  # Unregistered worker, ignore
                LOGGER.warning("Heartbeat from unreg worker %s", (host, port))

    def check_heartbeats(self):
        """Check if any workers are dead."""
        while not self.signals["shutdown"]:
            for _, val in self.workers.items():
                # Wait 10 secs
                if val.check_if_missing(time.time()):
                    if val.status != "dead":
                        # Worker is dead
                        LOGGER.info("DEAD WORKER: %s ", (
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
            LOGGER.info("ASSIGNED MAPPER")
        else:
            self.workers[worker_num].assign_reducer(task.file_paths,
                                                    task.task_id,
                                                    directory,
                                                    job,
                                                    task)
            LOGGER.info("ASSIGNED REDUCER")

    def get_worker(self, job, tmpdir):
        """Get next ready worker."""
        worker_num = None
        for key, val in self.workers.items():
            if val.status == "ready":
                worker_num = key
                LOGGER.info("PICKED WORKER %s", key)
                break
        # If there is an available worker
        if worker_num:
            self.workers[worker_num].update_status("busy")
            if job.state == "mapping":
                directory = tmpdir
            else:
                directory = str(job.out_dir)
            task = job.task_next()
            if task is None:  # No more tasks
                return
            self.assign_task(job, directory, worker_num, task)

    def handle_jobs(self, tmpdir):
        """Assign jobs to workers and run."""
        LOGGER.info("in jobs")

        while not self.signals["shutdown"]:
            if self.jobs.qsize() == 0:
                continue  # No jobs to process

            job = self.get_next_job(tmpdir)
            self.process_job(job)

    def process_job(self, job):
        """Process an individual job through mapping and reducing phases."""
        while not self.signals["shutdown"] and job.state != "f":
            if len(self.workers) == 0:
                continue  # No workers available

            ready = job.task_ready()
            if self.job_status == job.info["num_mappers"] and not ready:
                job.next_state(Path(job.interm_dir))
                self.job_status = 0
                if job.state == "reducing":
                    LOGGER.info("STARTING REDUCER")
            elif ready:
                self.get_worker(job, str(job.interm_dir))

        LOGGER.info("Job done, rm interdir: %s", job.interm_dir)
        shutil.rmtree(job.interm_dir)

    def get_next_job(self, tmpdir):
        """Get and set up next job."""
        job = self.jobs.get()
        job.next_state()
        LOGGER.info("Doing job %s, state: %s", job.id_, job.state)

        # Remove and recreate output directory
        if job.out_dir.exists() and job.out_dir.is_dir():
            shutil.rmtree(job.out_dir)
        job.out_dir.mkdir(parents=True, exist_ok=True)

        # Create intermediate directory
        job.interm_dir = Path(tmpdir) / f"job-{job.id_:05d}"
        if job.interm_dir.exists():
            LOGGER.info("Rm existing idir: %s", job.interm_dir)
            shutil.rmtree(job.interm_dir)
        job.interm_dir.mkdir(parents=True)
        LOGGER.info("Made interdir: %s", job.interm_dir)

        return job

    def msgs(self, message_dict):
        """Handle messages."""
        message = message_dict["message_type"]
        new_message = {}
        LOGGER.info("MANAGER RECEIVED MESSAGE %s", message)
        LOGGER.info("here")
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
            else:
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
            LOGGER.info("ADDED JOB %s", job.id_)

        # Job is done
        elif message == "finished":
            self.job_status += 1
            w = (message_dict["worker_host"], message_dict["worker_port"])
            self.workers[w].update_status("ready")


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
