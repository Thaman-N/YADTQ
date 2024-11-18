from kafka import KafkaProducer, KafkaConsumer
import json
import redis
import time
from config import KAFKA_BROKER_URL, JOB_QUEUE_TOPIC, WORKER_TOPICS, REDIS_URL
from result_store import ResultStore
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
        
        # Initialize task count dictionary with more robust tracking
        self.task_counts = {topic: 0 for topic in WORKER_TOPICS}

    def get_least_loaded_free_worker(self):
        free_workers = []
        for topic in WORKER_TOPICS:
            try:
                # Check worker status and task counts
                worker_status = self.result_store.get_worker_status(topic)
                self.logger.info(f"Worker {topic} status: {worker_status}, Current task count: {self.task_counts[topic]}")
                
                if worker_status == "free":
                    free_workers.append(topic)
            except Exception as e:
                self.logger.error(f"Error checking status for {topic}: {e}")
        
        if not free_workers:
            self.logger.warning("No free workers available! Trying all workers...")
            return WORKER_TOPICS[0]
        
        candidates = sorted(free_workers, key=lambda topic: self.task_counts[topic])
        min_count = self.task_counts[candidates[0]]
        
        # Select from workers with task count within small range of minimum
        eligible_workers = [w for w in candidates if self.task_counts[w] <= min_count + 1]
        
        selected_worker = random.choice(eligible_workers)
        
        self.logger.info(f"Selected worker {selected_worker}. Current task counts: {self.task_counts}")
        return selected_worker

    def get_job_status(self, job_id):
        try:
            job_tracking_key = f"job_tracking:{job_id}"
            job_data = self.redis_client.hgetall(job_tracking_key)
            
            if not job_data:
                return {
                    'status': 'Unknown',
                    'result': None
                }
            
            status = job_data.get(b'status', b'unknown').decode()
            status_mapping = {
                'queued': 'Pending',
                'in_progress': 'Processing',
                'processing': 'Processing',
                'completed': 'Completed',
                'success': 'Completed',
                'failed': 'Failed'
            }
            
            normalized_status = status_mapping.get(status, status.capitalize())
            
            # Try to get result
            result = None
            if b'result' in job_data:
                try:
                    result = json.loads(job_data[b'result'].decode())
                except:
                    result = job_data[b'result'].decode()
                    
            return {
                'status': normalized_status,
                'result': result
            }
            
        except Exception as e:
            self.logger.error(f"Error getting job status: {e}")
            return {
                'status': 'Error',
                'result': str(e)
            }
    
    def submit_job(self, job):
        # Assign a unique job ID if not already present
        if 'job_id' not in job:
            job['job_id'] = str(uuid.uuid4())

        # Existing job submission logic remains the same
        try:
            target_topic = self.get_least_loaded_free_worker()
        except Exception as e:
            self.logger.error(f"Failed to get worker: {e}")
            target_topic = WORKER_TOPICS[0]  # Fallback to first worker
        
        if target_topic:
            job['status'] = 'queued'
            job['assigned_worker'] = target_topic
            
            # Store job details in Redis for tracking and recovery
            try:
                self.redis_client.hset(f"job_tracking:{job['job_id']}", 
                                    mapping={
                                        'original_job': json.dumps(job),
                                        'status': 'queued',
                                        'timestamp': time.time()
                                    })
            except Exception as e:
                self.logger.error(f"Failed to store job in Redis: {e}")
            
            # Send job to Kafka
            try:
                self.producer.send(target_topic, job)
                self.producer.flush()
            except Exception as e:
                self.logger.error(f"Failed to send job to Kafka: {e}")
                traceback.print_exc()
                raise
            
            # Increment task count
            self.task_counts[target_topic] += 1
            
            self.logger.info(f"Job {job['job_id']} sent to {target_topic}. Task count: {self.task_counts[target_topic]}")
            
            return job  # Return the job dictionary with job_id
        else:
            self.logger.error("No workers available. Job cannot be processed.")
            raise Exception("No workers available")

    def update_task_count(self, topic, count=None):
        with threading.Lock():  # Add thread safety
            if count is not None:
                self.task_counts[topic] = max(0, count)  # Prevent negative numbers
            else:
                self.task_counts[topic] = max(0, self.task_counts[topic] - 1)
    
        self.logger.info(f"Updated task count for {topic}: {self.task_counts[topic]}")

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