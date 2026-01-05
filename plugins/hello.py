import logging

def hi():
    # This is better than print()
    logging.info("Hello from the hi_task!")
    return "Task completed successfully"

def add(x):
    return x+1