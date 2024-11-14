from flask import Flask, render_template, request, jsonify
from client import JobClient
import uuid

app = Flask(__name__)
task_processor = JobClient()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit_job', methods=['POST'])
def submit_job():
    try:
        job_type = request.form['job_type']
        arg1 = float(request.form['arg1'])
        arg2 = float(request.form['arg2'])
        num_operations = 1

        #job_ids = []

        for _ in range(num_operations):
            job_id = str(uuid.uuid4())
            job = {
                'job_id': job_id,
                'job_type': job_type,
                'args': [arg1, arg2],
                'kwargs': {}
            }

            if job_type == 'add':
                task_processor.submit_job(job_type, arg1, arg2)
            elif job_type == 'multiply':
                task_processor.submit_job(job_type, arg1, arg2)
            elif job_type == 'subtract':
                task_processor.submit_job(job_type, arg1, arg2)
            elif job_type == 'divide':
                task_processor.submit_job(job_type, arg1, arg2)
            elif job_type == 'flaky_operation':
                task_processor.submit_job(job_type, arg1, arg2)
            elif job_type == 'division_by_zero':
                task_processor.submit_job(job_type, arg1, 0)
            else:
                return jsonify({'error': 'Invalid job type'}), 400

            #job_ids.append(job_id)
            #status = task_processor.result_store.get_job_status(job_id)
            #print(f"Job {job_id} status after submission: {status['status']}")

        return jsonify({'job_id': job_id})
        #return jsonify({'job_ids': job_ids})  # Return all job IDs as a JSON response

    except KeyError as e:
        return jsonify({'error': f'Missing field: {str(e)}'}), 400
    except ValueError as e:
        return jsonify({'error': f'Invalid data: {str(e)}'}), 400

@app.route('/get_job_status', methods=['GET'])
def get_job_status():
    job_id = request.args.get('job_id')
    
    if not job_id:
        return jsonify({'error': 'Job ID is required'}), 400

    status = task_processor.get_job_status(job_id)
    return jsonify(status)

if __name__ == '__main__':
    app.run(debug=True, port=5001)
