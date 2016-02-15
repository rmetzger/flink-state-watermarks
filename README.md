# Flink 1.0 Example: Out-of-core state, Kafka, windows, event-time

This repository contains a job implemented the Flink 1.0 API, using some of the most important streaming features, to test everything is working properly.

Features tested:
 - **Out of core state**: use a **window** with huge number of keys
 - **Kafka**: generate the data into a kafka topic and read it from there again.
 - **use ot of order events**: The data generator is generating events with a variance.

 