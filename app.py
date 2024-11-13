from flask import Flask, render_template, request, jsonify
from client import JobClient
app = Flask(__name__)
task_processor = JobClient()
@app.route('/')
def index():
    return render_template('index.html')
@app.route('/submit_job', methods=['POST'])
def submit_job():
    job_type = request.form['job_type']
    arg1 = float(request.form['arg1'])
    arg2 = float(request.form['arg2'])
    job_id = task_processor.submit_job(job_type, arg1, arg2)
    return jsonify({'job_id': job_id})
@app.route('/get_job_status', methods=['GET'])
def get_job_status():
    job_id = request.args.get('job_id')
    status = task_processor.get_job_status(job_id)
    return jsonify(status)
if __name__ == '__main__':
    app.run(debug=True)
