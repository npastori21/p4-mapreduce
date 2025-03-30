"""MapReduce framework Worker node."""
import os
import tempfile
from pathlib import Path
import time
import threading
import hashlib
import shutil
import subprocess
import logging
import heapq
from contextlib import ExitStack
import click

from mapreduce.utils import tcp_client
from mapreduce.utils import udp_client
from mapreduce.utils import tcp_server
from mapreduce.utils import ThreadSafeOrderedDict


# Configure logging
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, filename="debug.log")


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # Worker attributes
        self.host_port = host, port
        self.ack = False
        self.task = None
        self.m_host_port = manager_host, manager_port
        self.signals = ThreadSafeOrderedDict()
        self.signals["shutdown"] = False

        messages_thread = threading.Thread(target=tcp_server, args=(
                                            self.host_port[0],
                                            self.host_port[1],
                                            self.signals,
                                            self.handle_messages))
        messages_thread.start()
        self.register()
        self.acknowledged = threading.Condition()

        heartbeats_thread = threading.Thread(target=self.heartbeat)
        heartbeats_thread.start()

        # Join threads when they stop running
        messages_thread.join()
        LOGGER.info("Messages thread")
        heartbeats_thread.join()
        LOGGER.info("Heartbeats thread")

    def register(self):
        """Register with manager."""
        message = {
            "message_type": "register",
            "worker_host": self.host_port[0],
            "worker_port": self.host_port[1],
        }
        try:
            tcp_client(message, self.m_host_port[0], self.m_host_port[1])
        except ConnectionRefusedError:
            LOGGER.info("REGISTER exception")

    def heartbeat(self):
        """Send heartbeat."""
        try:
            with self.acknowledged:
                while not self.ack and not self.signals["shutdown"]:
                    self.acknowledged.wait()
            while not self.signals["shutdown"]:
                LOGGER.info("Sending heartbeat to %s:%s", self.m_host_port[0], self.m_host_port[1])
                heartbeat = {
                    "message_type": "heartbeat",
                    "worker_host": self.host_port[0],
                    "worker_port": self.host_port[1],
                }
                try:
                    udp_client(heartbeat, self.m_host_port[0], self.m_host_port[1])
                    LOGGER.info("HEARTBEAT")
                except ConnectionRefusedError:
                    LOGGER.info("HEARTBEAT: Connection refused")
                    continue
                time.sleep(2)
        except Exception as e:
            LOGGER.error("Heartbeat thread crashed: %s", e, exc_info=True)

    def handle_messages(self, msg):
        """Handle worker messages."""
        message = msg.get("message_type")
        LOGGER.info("WORKER RECEIVED MESSAGE %s", message)

        # Shut down
        if message == "shutdown":
            self.signals["shutdown"] = True

        # Acknowledged
        elif message == "register_ack":
            self.ack = True
            with self.acknowledged:
                self.acknowledged.notify()

        # New mapping task
        elif message == "new_map_task":
            self.task = msg
            try:
                self.map()
            except FileNotFoundError:
                LOGGER.info("MAPPING exception")

        # New reducing task
        elif message == "new_reduce_task":
            self.task = msg
            try:
                self.reduce()
            except FileNotFoundError:
                LOGGER.info("REDUCING exception")

    def map(self):
        """Execute mapping task."""
        LOGGER.info("MAPPING")
        files = {}
        num = str(self.task["task_id"]).zfill(5)
        pre = f"mapreduce-local-task{num}-"
        with tempfile.TemporaryDirectory(prefix=pre)as tmpdir:
            for input_path in self.task["input_paths"]:
                with open(input_path, encoding='utf-8') as infile:
                    with subprocess.Popen(
                        [self.task["executable"]],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process, ExitStack() as stack:
                        LOGGER.info("MAPPING EXECUTABLE")
                        for num_parts in range(self.task["num_partitions"]):
                            filename = f"maptask{num}-part{
                                str(num_parts).zfill(5)}"
                            files[num_parts] = stack.enter_context(
                                (Path(tmpdir) / filename).open(
                                    "a", encoding='utf-8'))
                        for line in map_process.stdout:
                            key, _ = line.split("\t")
                            key = key.encode("utf-8")
                            hexdigest = hashlib.md5(key).hexdigest()
                            files[int(hexdigest, base=16) %
                                  self.task["num_partitions"]].write(line)
            for item in Path(tmpdir).iterdir():
                if item.is_file():
                    subprocess.run(["sort", "-o", item, item], check=True)
                    shutil.move(item, os.path.join(
                        self.task["output_directory"], item.name))
        self.send_message({
                "message_type": "finished",
                "task_id": self.task["task_id"],
                "worker_host": self.host_port[0],
                "worker_port": self.host_port[1]
            })

    def reduce(self):
        """Execute reducing task."""
        LOGGER.info("REDUCING")
        task_id = str(self.task["task_id"]).zfill(5)
        output_dir = Path(self.task["output_directory"])
        filename = f"part-{task_id}"
        input_paths = self.task["input_paths"]
        dest = output_dir / filename
        prefix = f"mapreduce-local-task{task_id}-"

        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            with ExitStack() as stack:
                out = stack.enter_context(
                    open(Path(tmpdir) / filename, 'w', encoding='utf-8'))
                instreams = [stack.enter_context(open(path, 'r',
                                                      encoding='utf-8'))
                             for path in input_paths]
                with subprocess.Popen(
                    [self.task["executable"]],
                    text=True,
                    stdin=subprocess.PIPE,
                    stdout=out,
                ) as reduce_process:
                    for line in heapq.merge(*instreams):
                        reduce_process.stdin.write(line)

                shutil.move(Path(tmpdir) / filename, dest)
        message = {
            "message_type": "finished",
            "task_id": self.task["task_id"],
            "worker_host": self.host_port[0],
            "worker_port": self.host_port[1]
        }
        self.send_message(message)

    def send_message(self, m):
        """Send message to manager."""
        LOGGER.info("WORKER TASK DONE")
        try:
            tcp_client(m, self.m_host_port[0], self.m_host_port[1])
        except ConnectionRefusedError:
            LOGGER.info("FINISHED JOB: Connection refused")


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
