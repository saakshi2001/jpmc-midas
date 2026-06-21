# JPMorgan Midas - Task 2

## Overview

This branch contains the implementation for **Task 2** of the JPMorgan Chase Software Engineering Virtual Experience Program.

The objective of this task was to extend the Midas transaction processing system by introducing Kafka-based event consumption and processing of transaction messages.

---

## Task Objective

Implement a Kafka consumer that:

- Listens to transaction events published through Kafka
- Deserializes incoming transaction messages
- Processes transactions within the Midas application workflow
- Integrates with the existing Spring Boot application

---

## Technologies Used

- Java
- Spring Boot
- Apache Kafka
- Maven
- JUnit

---

## Implementation Details

### Kafka Consumer

Added a Kafka consumer component responsible for:

- Subscribing to the configured Kafka topic
- Receiving transaction messages
- Mapping Kafka messages into `Transaction` objects
- Triggering transaction processing logic

### Configuration

Kafka configuration was added using Spring Boot application properties:

```properties
general.kafka-topic=<topic-name>
spring.kafka.bootstrap-servers=<kafka-server>
