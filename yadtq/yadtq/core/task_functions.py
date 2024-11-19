import time,random

def add(x, y):
    time.sleep(10)
    return x + y

def multiply(x, y):
    time.sleep(10)
    return x * y

def subtract(x, y):
    time.sleep(10)
    return x - y

def divide(x, y):
    time.sleep(10)
    if y == 0:
        raise ValueError("Division by zero is not allowed.")
    return x / y
    
def flaky_operation(x, y):
    """A deliberately unreliable operation that fails randomly"""
    if random.random() < 0.7:  # 70% chance of failure
        raise Exception("Random failure occurred!")
    time.sleep(5)
    return x + y

TASK_FUNCTIONS = {
    "add": add,
    "multiply": multiply,
    "subtract": subtract,
    "divide": divide,
    "flaky_operation":flaky_operation
}
