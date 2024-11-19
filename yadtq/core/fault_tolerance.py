import threading
import time
from yadtq.core.result_store import ResultStore
from yadtq.core.message_queue import JobProducer
from yadtq.core.config import WORKER_TOPICS

class FaultTolerantMonitor:
    def __init__(self, worker_id, check_interval=2, heartbeat_timeout=2):
        """
        Initialize a FaultTolerantMonitor instance for a specific worker.
        :param worker_id: The ID of the worker to monitor.
        :param check_interval: Time interval in seconds to check worker status.
        :param heartbeat_timeout: Maximum allowed time in seconds since the last heartbeat.
        """
        self.worker_id = worker_id
        self.result_store = ResultStore()
        self.job_producer = JobProducer()
        self.check_interval = check_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.last_status = None  # Track the last status of the worker

    def monitor_worker(self):
        """
        Monitor a single worker for status transitions and implement fault tolerance by
        re-queuing jobs when a worker transitions from 'processing' to 'inactive'.
        """
        while True:
            current_status = self.result_store.get_worker_status(self.worker_id)
            if current_status is None:
                print(f"[{self.worker_id}] No status found. Marking worker as inactive.")
                self.result_store.set_worker_status(self.worker_id, "inactive")
                continue

            # Detect transition from 'processing' to 'inactive'
            if self.last_status == "processing" and current_status == "inactive":
                print(f"[{self.worker_id}] Worker transitioned from 'processing' to 'inactive'.")
                self.handle_fault()

            self.last_status = current_status
            time.sleep(self.check_interval)

    def handle_fault(self):
        """
        Handle a fault by fetching the last task of the worker and re-queuing it.
        """
        last_task = self.result_store.get_worker_last_task(self.worker_id)
        if last_task:
            print(f"[{self.worker_id}] Re-queuing last task: {last_task['job_id']}")
            self.job_producer.submit_job(last_task)
        else:
            print(f"[{self.worker_id}] No last task found to re-queue.")

def start_fault_tolerant_monitor():
    """
    Start fault-tolerant monitoring for all workers defined in WORKER_TOPICS.
    """
    threads = []
    for worker_id in WORKER_TOPICS:
        monitor = FaultTolerantMonitor(worker_id)
        thread = threading.Thread(target=monitor.monitor_worker, daemon=True)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete (indefinitely running threads)
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Fault-Tolerant Monitor shutting down.")

if __name__ == "__main__":
    print("Starting Fault-Tolerant Worker Monitor...")
    start_fault_tolerant_monitor()
