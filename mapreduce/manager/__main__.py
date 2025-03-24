"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import threading
import socket
from mapreduce.utils import tcp_server


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

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        self.host = host
        self.port = port
        self.threads = []

        #setup temp directory
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            # FIXME: Add all code needed so that this `with` block doesn't end until the Manager shuts down.
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)
        time.sleep(120)

        #set tcp thread
        tcp_thread = threading.Thread(target=tcp_server)
        self.threads.append(tcp_thread)
        tcp_thread.start()

        #set worker thread
        worker_thread = threading.Thread(target=self.worker_heartbeats)
        self.threads.append(worker_thread)
        worker_thread.start()

        #fault tolerance thread
        fault_tolerance_thread = threading.Thread(target=self.fault_tolerance, daemon=True)
        fault_tolerance_thread.start()
        self.threads.append(fault_tolerance_thread)
        
        #join threads when they stop running
        for thread in self.threads:
            thread.join()
        



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
