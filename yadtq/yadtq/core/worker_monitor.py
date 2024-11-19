import threading
import time
from yadtq.core.result_store import ResultStore
from yadtq.core.config import WORKER_TOPICS

class WorkerMonitor:
    def __init__(self, worker_id, heartbeat_timeout=2):
        """
        Initialize a WorkerMonitor instance for a specific worker.
        :param worker_id: The ID of the worker to monitor.
        :param heartbeat_timeout: Maximum allowed time in seconds since the last heartbeat.
        """
        self.worker_id = worker_id
        self.result_store = ResultStore()
        self.heartbeat_timeout = heartbeat_timeout

    def monitor_worker(self):
        """
        Monitor a single worker for heartbeats and mark it as inactive if a heartbeat is not
        received within the timeout period.
        """
        while True:
            current_time = time.time()
            last_heartbeat_time = self.result_store.redis.get(f"worker:{self.worker_id}:heartbeat_timestamp")

            if last_heartbeat_time:
                last_heartbeat_time = float(last_heartbeat_time.decode())
                if current_time - last_heartbeat_time > self.heartbeat_timeout:
                    print(f"[{self.worker_id}] No heartbeat detected for {self.heartbeat_timeout} seconds. Marking as inactive.")
                    self.result_store.set_worker_status(self.worker_id, "inactive")
                else:
                    print(f"[{self.worker_id}] Heartbeat OK.")
            else:
                print(f"[{self.worker_id}] No heartbeat ever received. Marking as inactive.")
                self.result_store.set_worker_status(self.worker_id, "inactive")

            time.sleep(2)  # Check every second


def start_monitoring():
    threads = []
    for worker_id in WORKER_TOPICS:
        monitor = WorkerMonitor(worker_id)
        thread = threading.Thread(target=monitor.monitor_worker, daemon=True)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete (in this case, indefinitely running threads)
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Shutting down Worker Monitor...")


if __name__ == "__main__":
    print("Starting Worker Monitor...")
    start_monitoring()
