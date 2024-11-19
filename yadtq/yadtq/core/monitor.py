import time
import json
import redis
from kafka import KafkaProducer
from result_store import ResultStore
from config import WORKER_TOPICS, REDIS_URL, KAFKA_BROKER_URL


class WorkerMonitor:
    def __init__(self):
        self.redis = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
        self.result_store = ResultStore()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def get_worker_status(self, worker_topic):
        """
        Get the current status of a worker from Redis.
        """
        status_key = f"worker:{worker_topic}:status"
        status_data = self.redis.hget(status_key, "status")
        return status_data if status_data else None

    def handle_worker_inactive(self, worker):
        """
        Handle when a worker transitions to inactive. Fetch its last task and requeue it.
        """
        last_task_key = f"worker:{worker}:last_task"
        last_task_data = self.redis.get(last_task_key)

        if last_task_data:
            last_task = json.loads(last_task_data)
            print(f"Re-queuing last task for worker {worker}: {last_task}")
            self.producer.send("job_queue", last_task)
            self.producer.flush()
        else:
            print(f"No last task found for worker {worker}")

    def monitor_workers(self):
        """
        Monitor all workers and handle any status changes.
        """
        print("Starting worker monitor...")
        
        # Initialize a dictionary to track last known statuses
        last_statuses = {worker: self.get_worker_status(worker) for worker in WORKER_TOPICS}

        while True:
            for worker in WORKER_TOPICS:
                current_status = self.get_worker_status(worker)
                
                # Compare current status with last known status
                if current_status != last_statuses[worker]:
                    print(f"Worker {worker} status changed from {last_statuses[worker]} to {current_status}")
                    
                    # Handle transition from processing to inactive
                    if last_statuses[worker] == "processing" and current_status == "inactive":
                        self.handle_worker_inactive(worker)
                    
                    # Update last known status
                    last_statuses[worker] = current_status

            time.sleep(1)  # Poll every second


if __name__ == "__main__":
    monitor = WorkerMonitor()
    monitor.monitor_workers()
