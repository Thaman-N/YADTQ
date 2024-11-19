
import uuid

from yadtq.core.message_queue import JobProducer
from yadtq.core.result_store import ResultStore
class JobClient:
    def __init__(self):
        self.producer = JobProducer()
        self.result_store = ResultStore()
    def submit_job(self, job_type, arg1, arg2):
        job = {
            'job_type': job_type,
            'args': [arg1, arg2]
        }
        # Submit the job
        submitted_job = self.producer.submit_job(job)
        
        # Explicitly set the job status to 'queued' in the result store
        self.result_store.set_job_status(submitted_job['job_id'], 'queued')
        
        return submitted_job
    def get_job_status(self, job_id):
        return self.result_store.get_job_status(job_id)