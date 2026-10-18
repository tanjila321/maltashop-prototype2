# ADR 0001: Use RabbitMQ as the message broker
Status: accepted. Alternatives rejected: Apache Kafka, Amazon SQS/SNS, in-process events.
Criteria: operational simplicity for a team new to distributed systems, per-message acknowledgement and dead-lettering, topic routing, cost, and portability.
Decision and consequences: see Section 5.3 of the report.
