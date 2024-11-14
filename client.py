
import uuid
from message_queue import JobProducer
from result_store import ResultStore
class JobClient:
    def __init__(self):
        self.producer = JobProducer()
        self.result_store = ResultStore()
    def submit_job(self, job_type, *args, **kwargs):
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "job_type": job_type,
            "args": args,
            "kwargs": kwargs
        }
        self.producer.submit_job(job)
        return job_id
    def get_job_status(self, job_id):
        return self.result_store.get_job_status(job_id)