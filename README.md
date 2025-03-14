# YADTQ: Yet Another Distributed Task Queue

YADTQ is a scalable and fault-tolerant distributed task queue system built with Python. It leverages Kafka as the message broker and Redis as the result backend to efficiently manage and distribute asynchronous tasks across multiple worker nodes.

## Features

- **Scalability**: Seamlessly handle a growing number of tasks by adding more worker nodes.
- **Fault Tolerance**: Ensure task reliability with Kafka's robust messaging and Redis's persistent storage.
- **Asynchronous Processing**: Execute tasks asynchronously to improve application responsiveness and throughput.

## Installation

To set up YADTQ, follow these steps:

1. **Clone the repository**:

   ```bash
   git clone https://github.com/Thaman-N/YADTQ.git
   cd YADTQ
   ```

2. **Install dependencies**:

   Ensure you have Python installed, then run:

   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Kafka and Redis**:

   - **Kafka**: Follow the [Kafka Quickstart guide](https://kafka.apache.org/quickstart) to install and run Kafka.
   - **Redis**: Refer to the [Redis installation guide](https://redis.io/topics/quickstart) for setup instructions.

4. **Configure YADTQ**:

   Update the `config.py` file with your Kafka and Redis configurations:

   ```python
   KAFKA_BROKER_URL = 'localhost:9092'
   REDIS_HOST = 'localhost'
   REDIS_PORT = 6379
   ```

## Usage

1. **Define your tasks**:

   Implement your task functions in `task_functions.py`. For example:

   ```python
   def add(x, y):
       return x + y
   ```

2. **Start the task processor**:

   Run the task processor to listen for incoming tasks:

   ```bash
   python task_processor.py
   ```

3. **Submit tasks**:

   Use the client to enqueue tasks:

   ```bash
   python client.py
   ```

   Ensure you specify the task name and arguments as required.

## Contributing

We welcome contributions to enhance YADTQ. To contribute:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes with clear descriptions.
4. Submit a pull request to the main repository.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
