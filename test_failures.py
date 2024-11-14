# test_failures.py
import uuid
import time
from message_queue import JobProducer
from result_store import ResultStore

def test_division_by_zero():
    producer = JobProducer()
    result_store = ResultStore()
    
    # Create a job that will fail (division by zero)
    job_id = str(uuid.uuid4())
    job = {
        'job_id': job_id,
        'job_type': 'divide',
        'args': [10, 0],  # This will cause a division by zero error
        'kwargs': {}
    }
    
    print(f"Submitting job {job_id} (Division by zero)")
    producer.submit_job(job)
    
    # Monitor the job status
    for _ in range(20):
        status = result_store.get_job_status(job_id)
        print(f"\nJob Status: {status['status']}")
        print(f"Result: {status['result']}")
        
        if status['status'] != 'unknown':
            if status['status'] in ['failed', 'success']:
                break
        else:
            print(f"Job {job_id} status unknown, waiting for update...")
            
        # Monitor worker statuses
        for worker_topic in ['worker_1', 'worker_2', 'worker_3']:
            worker_status = result_store.get_worker_status(worker_topic)
            print(f"Worker {worker_topic} status: {worker_status}")
            
        time.sleep(2)

def test_flaky_operation():
    producer = JobProducer()
    result_store = ResultStore()
    
    job_id = str(uuid.uuid4())
    job = {
        'job_id': job_id,
        'job_type': 'flaky_operation',
        'args': [5, 3],
        'kwargs': {}
    }
    
    print(f"\nTesting flaky operation - Job ID: {job_id}")
    producer.submit_job(job)
    
    # Monitor the job status
    for _ in range(20):
        status = result_store.get_job_status(job_id)
        print(f"\nStatus: {status['status']}")
        print(f"Result: {status['result']}")
        
        # Monitor worker statuses
        for worker_topic in ['worker_1_topic', 'worker_2_topic', 'worker_3_topic']:
            worker_status = result_store.get_worker_status(worker_topic)
            print(f"Worker {worker_topic} status: {worker_status}")
        
        if status['status'] in ['failed', 'success']:
            break
            
        time.sleep(2)

if __name__ == "__main__":
    print("Testing division by zero...")
    test_division_by_zero()
    
    print("\nTesting flaky operation...")
    test_flaky_operation()