import random
def add(x, y):
    return x + y

def multiply(x, y):
    return x * y

def subtract(x, y):
    return x - y

def divide(x, y):
    if y == 0:
        raise ValueError("Division by zero is not allowed.")
    return x / y

def flaky_operation(x, y):
    """A deliberately unreliable operation that fails randomly"""
    if random.random() < 0.7:  # 70% chance of failure
        raise Exception("Random failure occurred!")
    return x + y

def division_by_zero(x, y):
    # Intentionally cause a division by zero error
    return x / 0  # This will raise ZeroDivisionError

TASK_FUNCTIONS = {
    "add": add,
    "multiply": multiply,
    "subtract": subtract,
    "divide": divide,
    "flaky_operation":flaky_operation,
    "division_by_zero":division_by_zero
}
#TASK_FUNCTIONS["flaky_operation"] = flaky_operation
