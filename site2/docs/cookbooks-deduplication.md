---
id: cookbooks-deduplication
title: Message deduplication
sidebar_label: Message deduplication 
---

<<<<<<< HEAD
**Message deduplication** is a feature of Pulsar that, when enabled, ensures that each message produced on Pulsar topics is persisted to disk *only once*, even if the message is produced more than once. Message deduplication essentially unburdens Pulsar applications of the responsibility of ensuring deduplication and instead handles it automatically on the server side.

Using message deduplication in Pulsar involves making some [configuration changes](#configuration) to your Pulsar brokers as well as some minor changes to the behavior of Pulsar [clients](#clients).

> For a more thorough theoretical explanation of message deduplication, see the [Concepts and Architecture](concepts-messaging.md#message-deduplication) document.


## How it works

Message deduplication can be enabled and disabled on a per-namespace basis. By default, it is *disabled* on all namespaces and can enabled in the following ways:

* Using the [`pulsar-admin namespaces`](#enabling) interface
* As a broker-level [default](#default) for all namespaces

## Configuration for message deduplication

You can configure message deduplication in Pulsar using the [`broker.conf`](reference-configuration.md#broker) configuration file. The following deduplication-related parameters are available:

Parameter | Description | Default
:---------|:------------|:-------
`brokerDeduplicationEnabled` | Sets the default behavior for message deduplication in the Pulsar [broker](reference-terminology.md#broker). If set to `true`, message deduplication will be enabled by default on all namespaces; if set to `false` (the default), deduplication will have to be [enabled](#enabling) and [disabled](#disabling) on a per-namespace basis. | `false`
`brokerDeduplicationMaxNumberOfProducers` | The maximum number of producers for which information will be stored for deduplication purposes. | `10000`
`brokerDeduplicationEntriesInterval` | The number of entries after which a deduplication informational snapshot is taken. A larger interval will lead to fewer snapshots being taken, though this would also lengthen the topic recovery time (the time required for entries published after the snapshot to be replayed). | `1000`
`brokerDeduplicationProducerInactivityTimeoutMinutes` | The time of inactivity (in minutes) after which the broker will discard deduplication information related to a disconnected producer. | `360` (6 hours)

### Setting the broker-level default {#default}

By default, message deduplication is *disabled* on all Pulsar namespaces. To enable it by default on all namespaces, set the `brokerDeduplicationEnabled` parameter to `true` and re-start the broker.

Regardless of the value of `brokerDeduplicationEnabled`, [enabling](#enabling) and [disabling](#disabling) via the CLI will override the broker-level default.

### Enabling message deduplication {#enabling}

You can enable message deduplication on specific namespaces, regardless of the the [default](#default) for the broker, using the [`pulsar-admin namespace set-deduplication`](reference-pulsar-admin.md#namespace-set-deduplication) command. You can use the `--enable`/`-e` flag and specify the namespace. Here's an example with <tenant>/<namespace>:
=======
When **Message deduplication** is enabled, it ensures that each message produced on Pulsar topics is persisted to disk *only once*, even if the message is produced more than once. Message deduplication is handled automatically on the server side. 

To use message deduplication in Pulsar, you need to configure your Pulsar brokers and clients.

## How it works

You can enable or disable message deduplication at the namespace level or the topic level. By default, it is disabled on all namespaces or topics. You can enable it in the following ways:

* Enable deduplication for all namespaces/topics at the broker-level.
* Enable deduplication for a specific namespace with the `pulsar-admin namespaces` interface.
* Enable deduplication for a specific topic with the `pulsar-admin topics` interface.

## Configure message deduplication

You can configure message deduplication in Pulsar using the [`broker.conf`](reference-configuration.md#broker) configuration file. The following deduplication-related parameters are available.

Parameter | Description | Default
:---------|:------------|:-------
`brokerDeduplicationEnabled` | Sets the default behavior for message deduplication in the Pulsar broker. If it is set to `true`, message deduplication is enabled on all namespaces/topics. If it is set to `false`, you have to enable or disable deduplication at the namespace level or the topic level. | `false`
`brokerDeduplicationMaxNumberOfProducers` | The maximum number of producers for which information is stored for deduplication purposes. | `10000`
`brokerDeduplicationEntriesInterval` | The number of entries after which a deduplication informational snapshot is taken. A larger interval leads to fewer snapshots being taken, though this lengthens the topic recovery time (the time required for entries published after the snapshot to be replayed). | `1000`
`brokerDeduplicationProducerInactivityTimeoutMinutes` | The time of inactivity (in minutes) after which the broker discards deduplication information related to a disconnected producer. | `360` (6 hours)

### Set default value at the broker-level

By default, message deduplication is *disabled* on all Pulsar namespaces/topics. To enable it on all namespaces/topics, set the `brokerDeduplicationEnabled` parameter to `true` and re-start the broker.

Even if you set the value for `brokerDeduplicationEnabled`, enabling or disabling via Pulsar admin CLI overrides the default settings at the broker-level.

### Enable message deduplication

Though message deduplication is disabled by default at the broker level, you can enable message deduplication for a specific namespace or topic using the [`pulsar-admin namespaces set-deduplication`](reference-pulsar-admin.md#namespace-set-deduplication) or the [`pulsar-admin topics set-deduplication`](reference-pulsar-admin.md#topic-set-deduplication) command. You can use the `--enable`/`-e` flag and specify the namespace/topic. 

The following example shows how to enable message deduplication at the namespace level.
>>>>>>> f773c602c... Test pr 10 (#27)

```bash
$ bin/pulsar-admin namespaces set-deduplication \
  public/default \
  --enable # or just -e
```

<<<<<<< HEAD
### Disabling message deduplication {#disabling}

You can disable message deduplication on a specific namespace using the same method shown [above](#enabling), except using the `--disable`/`-d` flag instead. Here's an example with <tenant>/<namespace>:
=======
### Disable message deduplication

Even if you enable message deduplication at the broker level, you can disable message deduplication for a specific namespace or topic using the [`pulsar-admin namespace set-deduplication`](reference-pulsar-admin.md#namespace-set-deduplication) or the [`pulsar-admin topics set-deduplication`](reference-pulsar-admin.md#topic-set-deduplication) command. Use the `--disable`/`-d` flag and specify the namespace/topic.

The following example shows how to disable message deduplication at the namespace level.
>>>>>>> f773c602c... Test pr 10 (#27)

```bash
$ bin/pulsar-admin namespaces set-deduplication \
  public/default \
  --disable # or just -d
```

<<<<<<< HEAD
## Message deduplication and Pulsar clients {#clients}

If you enable message deduplication in your Pulsar brokers, you won't need to make any major changes to your Pulsar clients. There are, however, two settings that you need to provide for your client producers:

1. The producer must be given a name
1. The message send timeout needs to be set to infinity (i.e. no timeout)

Instructions for [Java](#java), [Python](#python), and [C++](#cpp) clients can be found below.

### Java clients {#java}

To enable message deduplication on a [Java producer](client-libraries-java.md#producers), set the producer name using the `producerName` setter and set the timeout to 0 using the `sendTimeout` setter. Here's an example:
=======
## Pulsar clients

If you enable message deduplication in Pulsar brokers, you need complete the following tasks for your client producers:

1. Specify a name for the producer.
1. Set the message timeout to `0` (namely, no timeout).

The instructions for Java, Python, and C++ clients are different.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java clients-->

To enable message deduplication on a [Java producer](client-libraries-java.md#producers), set the producer name using the `producerName` setter, and set the timeout to `0` using the `sendTimeout` setter. 
>>>>>>> f773c602c... Test pr 10 (#27)

```java
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import java.util.concurrent.TimeUnit;

PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
Producer producer = pulsarClient.newProducer()
        .producerName("producer-1")
        .topic("persistent://public/default/topic-1")
        .sendTimeout(0, TimeUnit.SECONDS)
        .create();
```

<<<<<<< HEAD
### Python clients {#python}

To enable message deduplication on a [Python producer](client-libraries-python.md#producers), set the producer name using `producer_name` and the timeout to 0 using `send_timeout_millis`. Here's an example:
=======
<!--Python clients-->

To enable message deduplication on a [Python producer](client-libraries-python.md#producers), set the producer name using `producer_name`, and set the timeout to `0` using `send_timeout_millis`. 
>>>>>>> f773c602c... Test pr 10 (#27)

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer(
    "persistent://public/default/topic-1",
    producer_name="producer-1",
    send_timeout_millis=0)
```
<<<<<<< HEAD

### C++ clients {#cpp}

To enable message deduplication on a [C++ producer](client-libraries-cpp.md#producer), set the producer name using `producer_name` and the timeout to 0 using `send_timeout_millis`. Here's an example:
=======
<!--C++ clients-->

To enable message deduplication on a [C++ producer](client-libraries-cpp.md#producer), set the producer name using `producer_name`, and set the timeout to `0` using `send_timeout_millis`. 
>>>>>>> f773c602c... Test pr 10 (#27)

```cpp
#include <pulsar/Client.h>

std::string serviceUrl = "pulsar://localhost:6650";
std::string topic = "persistent://some-tenant/ns1/topic-1";
std::string producerName = "producer-1";

Client client(serviceUrl);

ProducerConfiguration producerConfig;
producerConfig.setSendTimeout(0);
producerConfig.setProducerName(producerName);

Producer producer;

Result result = client.createProducer(topic, producerConfig, producer);
```
<<<<<<< HEAD

=======
<!--END_DOCUSAURUS_CODE_TABS-->
>>>>>>> f773c602c... Test pr 10 (#27)
