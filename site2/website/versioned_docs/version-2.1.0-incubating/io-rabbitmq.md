---
id: version-2.1.0-incubating-io-rabbitmq
title: RabbitMQ Connector
<<<<<<< HEAD
sidebar_label: RabittMQ Connector
=======
sidebar_label: RabbitMQ Connector
>>>>>>> f773c602c... Test pr 10 (#27)
original_id: io-rabbitmq
---

## Source

<<<<<<< HEAD
The RabittMQ Source connector is used for receiving messages from a RabittMQ cluster and writing
=======
The RabbitMQ Source connector is used for receiving messages from a RabbitMQ cluster and writing
>>>>>>> f773c602c... Test pr 10 (#27)
messages to Pulsar topics.

### Source Configuration Options

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `connectionName` | `true` | `null` | A new broker connection name. |
| `amqUri` | `true` | `null` | An AMQP URI: host, port, username, password and virtual host. |
| `queueName` | `true` | `null` | RabbitMQ queue name. |

