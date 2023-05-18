import sys
import threading
import time
from typing import Dict

import psutil

from Client.Kafka import Kafka


class Monitor:
    def __init__(self):
        self.processes: Dict = {}
        self.threads = []

    def monitorProcess(self, pid, alias):
        self.processes[pid] = {
            "alias": alias,
            "obj": psutil.Process(pid),
            "start": time.time(),
            "CPU": [],  # process.cpu_percent()
            "RAM": [],  # process.memory_info().rss
            "VIRTUAL": [],  # process.memory_info().vms
            "FILE_HANDLES": [],  # p.num_fds()
            "SUBPROCESSES": [],  # len(process.children())
            "THREADS": []  # process.num_threads()
        }
        self.threads.append(threading.Thread(target=self.monitor, args=(pid, )))
        self.threads[-1].start()

    def monitor(self, pid):
        process = self.processes.get(pid, None)
        while process is not None:
            try:
                obj: psutil.Process = process["obj"]
                process["CPU"].append(obj.cpu_percent())
                memory = obj.memory_info()
                process["RAM"].append(memory.rss)
                process["VIRTUAL"].append(memory.vms)
                process["FILE_HANDLES"].append(obj.num_fds())
                process["SUBPROCESSES"].append(len(obj.children(recursive=True)))
                process["THREADS"].append(obj.num_threads())
                time.sleep(0.1)
                process = self.processes.get(pid, None)
            except BaseException as ex:
                self.stopMonitorProcess(pid)

    def stopMonitorProcess(self, pid):
        process = self.processes.get(pid, None)
        if process is None:
            return

        with open(f"monitor_{process['alias']}", "w") as f:
            f.write(f"""{process["start"]}
{time.time()}
{process["CPU"]}
{process["RAM"]}
{process["VIRTUAL"]}
{process["FILE_HANDLES"]}
{process["SUBPROCESSES"]}
{process["THREADS"]}""")

        del self.processes[pid]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("No arguments")
        sys.exit(1)

    address, topic = sys.argv[1], sys.argv[2]
    m = Monitor()

    consumer = Kafka.KafkaConsumerWrapper({
                'auto.offset.reset': 'latest',
                'allow.auto.create.topics': "true",
                'bootstrap.servers': address,
                'group.id': "-",
            }, [(topic, 7)])
    pids = []
    print("Dtarting")
    while True:
        msg = consumer.receiveBigMessage(partition=7, timeoutSeconds=1)
        if msg is None:
            continue

        print(msg.value().decode())
        if msg.value().decode().lower() == "exit":
            break

        data = msg.value().decode().split(",")
        pid, alias = int(data[0]), data[1]
        pids.append(pid)

        print(f"started process {pid} with alias {alias}")
        m.monitorProcess(pid, alias)

    for pid in pids:
        m.stopMonitorProcess(pid)

    print("closed")