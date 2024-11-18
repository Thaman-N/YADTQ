
import uuid
from message_queue import JobProducer
from result_store import ResultStore
class JobClient:
    def __init__(self):
        self.producer = JobProducer()
        self.result_store = ResultStore()
    def submit_job(self, job_type, arg1, arg2):
        job = {
            'job_type': job_type,
            'args': [arg1, arg2]
        }
        return self.producer.submit_job(job)
    def get_job_status(self, job_id):
        return self.producer.get_job_status(job_id)