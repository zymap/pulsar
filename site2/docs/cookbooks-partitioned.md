---
id: cookbooks-partitioned
title: Partitioned topics
sidebar_label: Partitioned Topics
---

<<<<<<< HEAD
By default, Pulsar topics are served by a single broker. Using only a single broker, however, limits a topic's maximum throughput. *Partitioned topics* are a special type of topic that can span multiple brokers and thus allow for much higher throughput. For an explanation of how partitioned topics work, see the [Partitioned Topics](concepts-messaging.md#partitioned-topics) concepts.

You can [publish](#publishing-to-partitioned-topics) to partitioned topics using Pulsar's client libraries and you can [create and manage](#managing-partitioned-topics) partitioned topics using Pulsar's [admin API](admin-api-overview.md).

## Publishing to partitioned topics

When publishing to partitioned topics, the only difference from non-partitioned topics is that you need to specify a [routing mode](concepts-messaging.md#routing-modes) when you create a new [producer](reference-terminology.md#producer). Examples for [Java](#java) are below.

### Java

Publishing messages to partitioned topics in the Java client works much like [publishing to normal topics](client-libraries-java.md#using-producers). The difference is that you need to specify either one of the currently available message routers or a custom router.

#### Routing mode

You can specify the routing mode in the ProducerConfiguration object that you use to configure your producer. You have three options:
=======
By default, Pulsar topics are served by a single broker. Using only a single broker limits a topic's maximum throughput. *Partitioned topics* are a special type of topic that can span multiple brokers and thus allow for much higher throughput. For an explanation of how partitioned topics work, see the [Partitioned Topics](concepts-messaging.md#partitioned-topics) concepts.

You can publish to partitioned topics using Pulsar client libraries and you can [create and manage](#managing-partitioned-topics) partitioned topics using Pulsar [admin API](admin-api-overview.md).

## Publish to partitioned topics

When publishing to partitioned topics, you do not need to explicitly specify a [routing mode](concepts-messaging.md#routing-modes) when you create a new producer. If you do not specify a routing mode, the round robin route mode is used. Take [Java](#java) as an example.

Publishing messages to partitioned topics in the Java client works much like [publishing to normal topics](client-libraries-java.md#using-producers). The difference is that you need to specify either one of the currently available message routers or a custom router.

### Routing mode

You can specify the routing mode in the ProducerConfiguration object that you use to configure your producer. Three options are available:
>>>>>>> f773c602c... Test pr 10 (#27)

* `SinglePartition`
* `RoundRobinPartition`
* `CustomPartition`

<<<<<<< HEAD
Here's an example:
=======
The following is an example:
>>>>>>> f773c602c... Test pr 10 (#27)

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-tenant/my-namespace/my-topic";

PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarBrokerRootUrl).build();
Producer<byte[]> producer = pulsarClient.newProducer()
        .topic(topic)
        .messageRoutingMode(MessageRoutingMode.SinglePartition)
        .create();
producer.send("Partitioned topic message".getBytes());
```

<<<<<<< HEAD
#### Custom message router
=======
### Custom message router
>>>>>>> f773c602c... Test pr 10 (#27)

To use a custom message router, you need to provide an implementation of the {@inject: javadoc:MessageRouter:/client/org/apache/pulsar/client/api/MessageRouter} interface, which has just one `choosePartition` method:

```java
public interface MessageRouter extends Serializable {
    int choosePartition(Message msg);
}
```

<<<<<<< HEAD
Here's a (not very useful!) router that routes every message to partition 10:
=======
The following router routes every message to partition 10:
>>>>>>> f773c602c... Test pr 10 (#27)

```java
public class AlwaysTenRouter implements MessageRouter {
    public int choosePartition(Message msg) {
        return 10;
    }
}
```

With that implementation in hand, you can send

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-tenant/my-cluster-my-namespace/my-topic";

PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarBrokerRootUrl).build();
Producer<byte[]> producer = pulsarClient.newProducer()
        .topic(topic)
        .messageRouter(new AlwaysTenRouter())
        .create();
producer.send("Partitioned topic message".getBytes());
```

<<<<<<< HEAD
## Managing partitioned topics

You can use Pulsar's [admin API](admin-api-overview.md) to create and manage [partitioned topics](admin-api-partitioned-topics.md).
=======
### How to choose partitions when using a key
If a message has a key, it supersedes the round robin routing policy. The following example illustrates how to choose partition when you use a key.

```java
// If the message has a key, it supersedes the round robin routing policy
        if (msg.hasKey()) {
            return signSafeMod(hash.makeHash(msg.getKey()), topicMetadata.numPartitions());
        }

        if (isBatchingEnabled) { // if batching is enabled, choose partition on `partitionSwitchMs` boundary.
            long currentMs = clock.millis();
            return signSafeMod(currentMs / partitionSwitchMs + startPtnIdx, topicMetadata.numPartitions());
        } else {
            return signSafeMod(PARTITION_INDEX_UPDATER.getAndIncrement(this), topicMetadata.numPartitions());
        }
```        

## Manage partitioned topics

You can use Pulsar [admin API](admin-api-overview.md) to create and manage [partitioned topics](admin-api-partitioned-topics.md).
>>>>>>> f773c602c... Test pr 10 (#27)
