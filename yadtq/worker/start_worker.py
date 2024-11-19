import sys
import signal

from yadtq.core.task_processor import TaskProcessor  # Import from core

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 start_worker.py <worker_topic>")
        sys.exit(1)
    
    topic = sys.argv[1]
    task_processor = TaskProcessor(topic)
    
    def signal_handler(signum, frame):
        print(f"\nReceived shutdown signal. Stopping worker {topic}...")
        #task_processor.stop()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)   # Handle Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Handle termination requests
    
    try:
        task_processor.run()
    except Exception as e:
        print(f"Error in worker {topic}: {e}")
        task_processor.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
