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
        self.threads = []
        self.job_id = 0
        self.job_status = 0 # Is job completed?
        self.jobs = queue.Queue()
        self.workers = ThreadSafeOrderedDict() 
        self.signals = ThreadSafeOrderedDict()
        self.signals["shutdown"] = False

       
        with tempfile.TemporaryDirectory(prefix='mapreduce-shared-') as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            # UDP thread for heartbeats (port =+ 1 to prevent overlap)
            worker_thread = threading.Thread(target=udp_server,args=(self.host,
                                                    self.port,
                                                    self.signals,
                                                    self.handle_heartbeats))
            self.threads.append(worker_thread)
            worker_thread.start()

            # TCP thread for messages
            messages_thread = threading.Thread(target=tcp_server,args=(self.host,
                                                    self.port,
                                                    self.signals,
                                                    self.handle_messages,))
            self.threads.append(messages_thread)
            messages_thread.start()

            # job_thread = threading.Thread(target=self.handle_jobs, daemon=True)
            # self.threads.append(job_thread)
            # job_thread.start()

            # Thread for checking worker statuses
            workers_status_thread = threading.Thread(target=self.check_heartbeats)
            workers_status_thread.start()

            # TODO: not sure if this is needed
            # Fault tolerance thread
            # fault_tolerance_thread = threading.Thread(target=self.fault_tolerance, daemon=True)
            # self.threads.append(fault_tolerance_thread)
            # fault_tolerance_thread.start()
            LOGGER.info("starting jobs")
            self.handle_jobs(tmpdir)
            
        # LOGGER.info("JOBS COMPLETED")
            # Join threads when they stop running
            # for thread in self.threads:
            #     thread.join()
            worker_thread.join()
            LOGGER.info("workers thread")
            # job_thread.join()
            LOGGER.info("jobs thread")
            messages_thread.join()
            LOGGER.info("messages thread")
            workers_status_thread.join()
            LOGGER.info("status thread")

        LOGGER.info("Cleaned tmpdir %s", tmpdir)

    def handle_heartbeats(self, worker):
            """Update worker heartbeat."""
            if worker["message_type"] == "heartbeat" and len(self.workers) > 0:
                LOGGER.info("MANAGER RECEIVED HEARTBEAT")
                # Get worker's host and port
                host = worker["worker_host"]
                port = worker["worker_port"]
                if (host, port) in self.workers:
                    # If worker is alive, update heartbeat
                    if self.workers[(host, port)].status != "dead":
                        self.workers[(host, port)].update_heartbeat(time.time())
                        LOGGER.info("Updated heartbeat for worker: %s", (host, port))
                else: # Unregistered worker, ignore
                    LOGGER.warning("Received heartbeat from unregistered worker: %s", (host, port))
                    return

    def check_heartbeats(self):
            """Check if any workers are dead."""
            while not self.signals["shutdown"]:
                for _, val in self.workers.items():
                    # Wait 10 secs
                    if val.check_if_missing(time.time()):
                        LOGGER.info("Worker %s last heartbeat: %s", val.host, val.last_heartbeat)
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
                if task is None: # No more tasks
                    return
                self.assign_task(job, directory, worker_num, task)


    def handle_jobs(self, manager_dir):
            """Assign jobs to workers and run."""
            LOGGER.info("in jobs")
            while not self.signals["shutdown"]:
                # While not shutdown and there are more jobs
                if self.jobs.qsize() > 0:
                    # Get job
                    job = self.jobs.get()
                    job.next_state()
                    LOGGER.info("Processing job %s, current state: %s", job.id_, job.state)
                    # Delete directory if it already exists
                    if job.out_dir.exists() and job.out_dir.is_dir():
                        shutil.rmtree(job.out_dir)
                    job.out_dir.mkdir(parents=True, exist_ok=True)
                    prefix = f"{manager_dir}-job{job.id_:05d}"
                    self.job_status = 0
                    info = job.info["num_mappers"]
                    
                    #Create intermediate directory
                    intermediate_dir = Path(manager_dir) / f"job-{job.id_:05d}"
                    if intermediate_dir.exists():
                        LOGGER.info("Removing existing intermediate directory: %s", intermediate_dir)
                        shutil.rmtree(intermediate_dir)
                    intermediate_dir.mkdir(parents=True)
                    LOGGER.info("Created intermediate directory: %s", intermediate_dir)
                    # Create shared directory

                    while not self.signals["shutdown"] and job.state != "f":
                        if len(self.workers) > 0:
                            if self.job_status == info and not job.task_ready():
                                # Assign job
                                job.next_state(Path(intermediate_dir))
                                self.job_status = 0
                                info = 0
                                if job.state == "reducing":
                                    LOGGER.info("STARTING REDUCER")
                                    info = job.info["num_reducers"]
                            elif job.task_ready():
                                self.get_worker(job, str(intermediate_dir))
                    LOGGER.info("Job done, removing existing intermediate directory: %s", intermediate_dir)
                    shutil.rmtree(intermediate_dir)

    def handle_messages(self, message_dict):
            """Handle messages."""
            message = message_dict["message_type"]
            new_message = {}
            LOGGER.info("MANAGER RECEIVED MESSAGE %s", message)

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
