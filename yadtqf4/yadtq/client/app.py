from flask import Flask, render_template, request, jsonify

from client import JobClient
import redis,json
from yadtq.core.config import REDIS_URL

app = Flask(__name__)
task_processor = JobClient()
redis_client = redis.StrictRedis.from_url(REDIS_URL)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit_job', methods=['POST'])
def submit_job():
    try:
        job_type = request.form['job_type']
        arg1 = float(request.form['arg1'])
        arg2 = float(request.form['arg2'])

        if job_type == 'add':
            job = task_processor.submit_job(job_type, arg1, arg2)
        elif job_type == 'multiply':
            job = task_processor.submit_job(job_type, arg1, arg2)
        elif job_type == 'subtract':
            job = task_processor.submit_job(job_type, arg1, arg2)
        elif job_type == 'divide':
            job = task_processor.submit_job(job_type, arg1, arg2)
        elif job_type == 'flaky_operation':
            job = task_processor.submit_job(job_type, arg1, arg2)
        elif job_type == 'division_by_zero':
            job = task_processor.submit_job(job_type, arg1, 0)
        else:
            return jsonify({'error': 'Invalid job type'}), 400

        # Return job_id from the message queue submission
        return jsonify({'job_id': job.get('job_id')})

    except KeyError as e:
        return jsonify({'error': f'Missing field: {str(e)}'}), 400
    except ValueError as e:
        return jsonify({'error': f'Invalid data: {str(e)}'}), 400

@app.route('/get_job_status', methods=['GET'])
def get_job_status():
    job_id = request.args.get('job_id')
    
    if not job_id:
        return jsonify({'error': 'Job ID is required'}), 400

    try:
        status_result = task_processor.get_job_status(job_id)
        return jsonify(status_result)
    except Exception as e:
        return jsonify({
            'status': 'Error',
            'result': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)
