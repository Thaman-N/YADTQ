#final
from kafka import KafkaProducer, KafkaConsumer
import json
import redis
import time
from yadtq.core.config import KAFKA_BROKER_URL, JOB_QUEUE_TOPIC, WORKER_TOPICS, REDIS_URL
from yadtq.core.result_store import ResultStore
import logging
import uuid
import traceback
import threading,random

class JobProducer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.result_store = ResultStore()
        self.redis_client = redis.StrictRedis.from_url(REDIS_URL)
        
        # Initialize Redis task counts for all worker topics
        for topic in WORKER_TOPICS:
            count_key = f"worker:{topic}:task_count"
            if not self.redis_client.exists(count_key):
                self.redis_client.set(count_key, 0)


    def get_least_loaded_free_worker(self):
        try:
            # Get set of currently active workers
            active_workers = self.redis_client.smembers("active_workers")
            active_workers = [w.decode() for w in active_workers]
            
            if not active_workers:
                self.logger.error("No active workers found!")
                raise Exception("No active workers available")
            
            worker_loads = {}
            free_workers = []
            current_time = time.time()
            
            for topic in active_workers:
                try:
                    # Get worker status and heartbeat
                    worker_status = self.result_store.get_worker_status(topic)
                    worker_heartbeat = self.redis_client.hget(f"worker:{topic}:heartbeat", "last_heartbeat")
                    count_key = f"worker:{topic}:task_count"
                    task_count = int(self.redis_client.get(count_key) or 0)
                    
                    # Get last assigned time, default to 0 if not exists
                    last_assigned_key = f"worker:{topic}:last_assigned"
                    last_assigned_time = float(self.redis_client.get(last_assigned_key) or 0)
                    
                    # Consider worker active if heartbeat exists and is recent
                    is_active = worker_heartbeat and \
                            (current_time - float(worker_heartbeat.decode())) < 30
                    
                    if is_active:
                        # Calculate worker score: combine task count and time since last assignment
                        score = task_count + max(0, (current_time - last_assigned_time) * -0.1)
                        worker_loads[topic] = score
                        
                        if worker_status in ['free', 'initializing']:
                            free_workers.append(topic)
                            
                    self.logger.info(f"Worker {topic} status: {worker_status}, " \
                                f"Task count: {task_count}, Active: {is_active}")
                        
                except Exception as e:
                    self.logger.error(f"Error checking status for {topic}: {e}")
                    continue
            
            if not worker_loads:
                raise Exception("No active workers available")
            
            # If we have free workers, select the least loaded one
            if free_workers:
                selected_worker = min(free_workers, key=lambda w: worker_loads[w])
                self.logger.info(f"Selected free worker {selected_worker} with score {worker_loads[selected_worker]}")
            else:
                # If no free workers, select the least loaded active worker
                selected_worker = min(worker_loads.keys(), key=lambda w: worker_loads[w])
                self.logger.info(f"No free workers, selected least loaded worker {selected_worker}")
            
            # Update last assigned time for the selected worker
            self.redis_client.set(f"worker:{selected_worker}:last_assigned", current_time)
            
            return selected_worker
            
        except Exception as e:
            self.logger.error(f"Error finding available worker: {e}")
            raise Exception("No workers available")
    def submit_job(self, job):
        if 'job_id' not in job:
            job['job_id'] = str(uuid.uuid4())

        try:
            target_topic = self.get_least_loaded_free_worker()
            
            job['status'] = 'queued'
            job['assigned_worker'] = target_topic
            
            # Store job tracking info
            self.redis_client.hset(f"job_tracking:{job['job_id']}", 
                                mapping={
                                    'original_job': json.dumps(job),
                                    'status': 'queued',
                                    'timestamp': time.time()
                                })
            
            # Send job to Kafka
            self.producer.send(target_topic, job)
            self.producer.flush()
            
            # Increment task count in Redis
            count_key = f"worker:{target_topic}:task_count"
            self.redis_client.incr(count_key)
            current_count = int(self.redis_client.get(count_key))
            
            self.logger.info(f"Job {job['job_id']} sent to {target_topic}. Task count: {current_count}")
            return job
            
        except Exception as e:
            self.logger.error(f"Failed to submit job: {e}")
            traceback.print_exc()
            raise

    def update_task_count(self, topic, count=None):
        """Update task count in Redis"""
        try:
            count_key = f"worker:{topic}:task_count"
            if count is not None:
                self.redis_client.set(count_key, max(0, count))
            else:
                # Decrement but don't go below 0
                self.redis_client.set(count_key, max(0, int(self.redis_client.get(count_key) or 0) - 1))
            
            current_count = int(self.redis_client.get(count_key) or 0)
            self.logger.info(f"Updated task count for {topic}: {current_count}")
        except Exception as e:
            self.logger.error(f"Error updating task count: {e}")

class JobConsumer:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.redis_client = redis.StrictRedis.from_url(REDIS_URL)

    def consume_jobs(self):
        for message in self.consumer:
            # Retrieve and update job tracking details in Redis
            job = message.value
            tracking_id = job.get('tracking_id')
            
            if tracking_id:
                # Update job status in Redis tracking
                self.redis_client.hset(f"job_tracking:{tracking_id}", 
                                       mapping={
                                           'status': 'in_progress',
                                           'timestamp': time.time()
                                       })
            
            yield job