import time
import sys
import logging
import random

def submit_test_job():
    from message_queue import JobProducer
    
    producer = JobProducer()
    job = {
        'job_id': 'test-job-001',
        'job_type': 'slow_task',
        'args': ['test-job-001'],
        'retry_count': 0
    }
    
    print("Submitting test job...")
    producer.submit_job(job)
    print("Job submitted. You can now simulate worker interruption.")

if __name__ == "__main__":
    submit_test_job()
