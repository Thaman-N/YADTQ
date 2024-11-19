import random,time
def add(x, y):
    time.sleep(5)
    return x + y

def multiply(x, y):
    time.sleep(5)
    return x * y

def subtract(x, y):
    time.sleep(15)
    return x - y

def divide(x, y):
    if y == 0:
        raise ValueError("Division by zero is not allowed.")
    time.sleep(5)
    return x / y

def flaky_operation(x, y):
    """A deliberately unreliable operation that fails randomly"""
    if random.random() < 0.7:  # 70% chance of failure
        raise Exception("Random failure occurred!")
    time.sleep(5)
    return x + y

def division_by_zero(x, y):
    # Intentionally cause a division by zero error
    time.sleep(5)
    return x / 0  # This will raise ZeroDivisionError

def slow_task(job_id):
    """
    A deliberately slow task that can be interrupted
    Simulates work with periodic sleeps
    """
    print(f"Starting job {job_id}")
    for i in range(10):
        # Simulate work
        print(f"Job {job_id}: Working on step {i+1}")
        time.sleep(random.uniform(3, 5))  # Random sleep between 1-3 seconds
        
        # Simulate potential failure
        if random.random() < 0.3:  # 30% chance of simulated failure
            raise Exception(f"Simulated failure in job {job_id} at step {i+1}")
    
    return f"Job {job_id} completed successfully"

TASK_FUNCTIONS = {
    "add": add,
    "multiply": multiply,
    "subtract": subtract,
    "divide": divide,
    "flaky_operation":flaky_operation,
    "division_by_zero":division_by_zero,
    'slow_task': slow_task
}
#TASK_FUNCTIONS["flaky_operation"] = flaky_operation
