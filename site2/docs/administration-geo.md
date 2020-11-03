---
id: administration-geo
title: Pulsar geo-replication
sidebar_label: Geo-replication
---

*Geo-replication* is the replication of persistently stored message data across multiple clusters of a Pulsar instance.

<<<<<<< HEAD
## How it works
=======
## How geo-replication works
>>>>>>> f773c602c... Test pr 10 (#27)

The diagram below illustrates the process of geo-replication across Pulsar clusters:

![Replication Diagram](assets/geo-replication.png)

<<<<<<< HEAD
In this diagram, whenever producers **P1**, **P2**, and **P3** publish messages to the topic **T1** on clusters **Cluster-A**, **Cluster-B**, and **Cluster-C**, respectively, those messages are instantly replicated across clusters. Once replicated, consumers **C1** and **C2** can consume those messages from their respective clusters.

Without geo-replication, consumers **C1** and **C2** wouldn't be able to consume messages published by producer **P3**.

## Geo-replication and Pulsar properties

Geo-replication must be enabled on a per-tenant basis in Pulsar. Geo-replication can be enabled between clusters only when a tenant has been created that allows access to both clusters.

Although geo-replication must be enabled between two clusters, it's actually managed at the namespace level. You must do the following to enable geo-replication for a namespace:

* [Create a global namespace](#creating-global-namespaces)
* Configure that namespace to replicate between two or more provisioned clusters

Any message published on *any* topic in that namespace will then be replicated to all clusters in the specified set.

## Local persistence and forwarding

When messages are produced on a Pulsar topic, they are first persisted in the local cluster and then forwarded asynchronously to the remote clusters.

In normal cases, when there are no connectivity issues, messages are replicated immediately, at the same time as they are dispatched to local consumers. Typically, end-to-end delivery latency is defined by the network [round-trip time](https://en.wikipedia.org/wiki/Round-trip_delay_time) (RTT) between the remote regions.

Applications can create producers and consumers in any of the clusters, even when the remote clusters are not reachable (like during a network partition).

> #### Subscriptions are local to a cluster
> While producers and consumers can publish to and consume from any cluster in a Pulsar instance, subscriptions are local to the clusters in which they are created and cannot be transferred between clusters. If you do need to transfer a subscription, you’ll need to create a new subscription in the desired cluster.

In the example in the image above, the topic **T1** is being replicated between 3 clusters, **Cluster-A**, **Cluster-B**, and **Cluster-C**.

All messages produced in any cluster will be delivered to all subscriptions in all the other clusters. In this case, consumers **C1** and **C2** will receive all messages published by producers **P1**, **P2**, and **P3**. Ordering is still guaranteed on a per-producer basis.

## Configuring replication

As stated [above](#geo-replication-and-pulsar-properties), geo-replication in Pulsar is managed at the [tenant](reference-terminology.md#tenant) level.

### Granting permissions to properties

To establish replication to a cluster, the tenant needs permission to use that cluster. This permission can be granted when the tenant is created or later on.

At creation time, specify all the intended clusters:

```shell
$ bin/pulsar-admin properties create my-tenant \
=======
In this diagram, whenever **P1**, **P2**, and **P3** producers publish messages to the **T1** topic on **Cluster-A**, **Cluster-B**, and **Cluster-C** clusters respectively, those messages are instantly replicated across clusters. Once the messages are replicated, **C1** and **C2** consumers can consume those messages from their respective clusters.

Without geo-replication, **C1** and **C2** consumers are not able to consume messages that **P3** producer publishes.

## Geo-replication and Pulsar properties

You must enable geo-replication on a per-tenant basis in Pulsar. You can enable geo-replication between clusters only when a tenant is created that allows access to both clusters.

Although geo-replication must be enabled between two clusters, actually geo-replication is managed at the namespace level. You must complete the following tasks to enable geo-replication for a namespace:

* [Enable geo-replication namespaces](#enable-geo-replication-namespaces)
* Configure that namespace to replicate across two or more provisioned clusters

Any message published on *any* topic in that namespace is replicated to all clusters in the specified set.

## Local persistence and forwarding

When messages are produced on a Pulsar topic, messages are first persisted in the local cluster, and then forwarded asynchronously to the remote clusters.

In normal cases, when connectivity issues are none, messages are replicated immediately, at the same time as they are dispatched to local consumers. Typically, the network [round-trip time](https://en.wikipedia.org/wiki/Round-trip_delay_time) (RTT) between the remote regions defines end-to-end delivery latency.

Applications can create producers and consumers in any of the clusters, even when the remote clusters are not reachable (like during a network partition).

Producers and consumers can publish messages to and consume messages from any cluster in a Pulsar instance. However, subscriptions cannot only be local to the cluster where the subscriptions are created but also can be transferred between clusters after replicated subscription is enabled. Once replicated subscription is enabled, you can keep subscription state in synchronization. Therefore, a topic can be asynchronously replicated across multiple geographical regions. In case of failover, a consumer can restart consuming messages from the failure point in a different cluster.

In the aforementioned example, the **T1** topic is replicated among three clusters, **Cluster-A**, **Cluster-B**, and **Cluster-C**.

All messages produced in any of the three clusters are delivered to all subscriptions in other clusters. In this case, **C1** and **C2** consumers receive all messages that **P1**, **P2**, and **P3** producers publish. Ordering is still guaranteed on a per-producer basis.

## Configure replication

As stated in [Geo-replication and Pulsar properties](#geo-replication-and-pulsar-properties) section, geo-replication in Pulsar is managed at the [tenant](reference-terminology.md#tenant) level.

### Grant permissions to properties

To replicate to a cluster, the tenant needs permission to use that cluster. You can grant permission to the tenant when you create the tenant or grant later.

Specify all the intended clusters when you create a tenant:

```shell
$ bin/pulsar-admin tenants create my-tenant \
>>>>>>> f773c602c... Test pr 10 (#27)
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east,us-cent
```

To update permissions of an existing tenant, use `update` instead of `create`.

<<<<<<< HEAD
### Creating global namespaces

Replication must be used with *global* topics, meaning topics that belong to a global namespace and are thus not tied to any particular cluster.

Global namespaces need to be created in the `global` virtual cluster. For example:
=======
### Enable geo-replication namespaces

You can create a namespace with the following command sample.
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
$ bin/pulsar-admin namespaces create my-tenant/my-namespace
```

Initially, the namespace is not assigned to any cluster. You can assign the namespace to clusters using the `set-clusters` subcommand:

```shell
$ bin/pulsar-admin namespaces set-clusters my-tenant/my-namespace \
  --clusters us-west,us-east,us-cent
```

<<<<<<< HEAD
The replication clusters for a namespace can be changed at any time, with no disruption to ongoing traffic. Replication channels will be immediately set up or stopped in all the clusters as soon as the configuration changes.

### Using global topics

Once you've created a global namespace, any topics that producers or consumers create within that namespace will be global. Typically, each application will use the `serviceUrl` for the local cluster.

#### Selective replication

By default, messages are replicated to all clusters configured for the namespace. You can restrict replication selectively by specifying a replication list for a message. That message will then be replicated only to the subset in the replication list.

Below is an example for the [Java API](client-libraries-java.md). Note the use of the `setReplicationClusters` method when constructing the {@inject: javadoc:Message:/client/org/apache/pulsar/client/api/Message} object:
=======
You can change the replication clusters for a namespace at any time, without disruption to ongoing traffic. Replication channels are immediately set up or stopped in all clusters as soon as the configuration changes.

### Use topics with geo-replication

Once you create a geo-replication namespace, any topics that producers or consumers create within that namespace is replicated across clusters. Typically, each application uses the `serviceUrl` for the local cluster.

#### Selective replication

By default, messages are replicated to all clusters configured for the namespace. You can restrict replication selectively by specifying a replication list for a message, and then that message is replicated only to the subset in the replication list.

The following is an example for the [Java API](client-libraries-java.md). Note the use of the `setReplicationClusters` method when you construct the {@inject: javadoc:Message:/client/org/apache/pulsar/client/api/Message} object:
>>>>>>> f773c602c... Test pr 10 (#27)

```java
List<String> restrictReplicationTo = Arrays.asList(
        "us-west",
        "us-east"
);

Producer producer = client.newProducer()
        .topic("some-topic")
        .create();

producer.newMessage()
        .value("my-payload".getBytes())
        .setReplicationClusters(restrictReplicationTo)
        .send();
```

#### Topic stats

<<<<<<< HEAD
Topic-specific statistics for global topics are available via the [`pulsar-admin`](reference-pulsar-admin.md) tool and {@inject: rest:REST:/} API:
=======
Topic-specific statistics for geo-replication topics are available via the [`pulsar-admin`](reference-pulsar-admin.md) tool and {@inject: rest:REST:/} API:
>>>>>>> f773c602c... Test pr 10 (#27)

```shell
$ bin/pulsar-admin persistent stats persistent://my-tenant/my-namespace/my-topic
```

<<<<<<< HEAD
Each cluster reports its own local stats, including incoming and outgoing replication rates and backlogs.

#### Deleting a global topic

Given that global topics exist in multiple regions, it's not possible to directly delete a global topic. Instead, you should rely on automatic topic garbage collection.

In Pulsar, a topic is automatically deleted when it's no longer used, that is to say, when no producers or consumers are connected *and* there are no subscriptions *and* no more messages are kept for retention. For global topics, each region will use a fault-tolerant mechanism to decide when it's safe to delete the topic locally.

You can explicitly disable topic garbage collection by setting `brokerDeleteInactiveTopicsEnabled` to `false` in your [broker configuration](reference-configuration#broker).

To delete a global topic, close all producers and consumers on the topic and delete all its local subscriptions in every replication cluster. When Pulsar determines that no valid subscription for the topic remains across the system, it will garbage collect the topic.
=======
Each cluster reports its own local stats, including the incoming and outgoing replication rates and backlogs.

#### Delete a geo-replication topic

Given that geo-replication topics exist in multiple regions, directly deleting a geo-replication topic is not possible. Instead, you should rely on automatic topic garbage collection.

In Pulsar, a topic is automatically deleted when the topic meets the following three conditions:
- no producers or consumers are connected to it;
- no subscriptions to it;
- no more messages are kept for retention. 
For geo-replication topics, each region uses a fault-tolerant mechanism to decide when deleting the topic locally is safe.

You can explicitly disable topic garbage collection by setting `brokerDeleteInactiveTopicsEnabled` to `false` in your [broker configuration](reference-configuration.md#broker).

To delete a geo-replication topic, close all producers and consumers on the topic, and delete all of its local subscriptions in every replication cluster. When Pulsar determines that no valid subscription for the topic remains across the system, it will garbage collect the topic.

## Replicated subscriptions

Pulsar supports replicated subscriptions, so you can keep subscription state in sync, within a sub-second timeframe, in the context of a topic that is being asynchronously replicated across multiple geographical regions.

In case of failover, a consumer can restart consuming from the failure point in a different cluster. 

### Enable replicated subscription

Replicated subscription is disabled by default. You can enable replicated subscription when creating a consumer. 

```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("my-topic")
            .subscriptionName("my-subscription")
            .replicateSubscriptionState(true)
            .subscribe();
```

### Advantages

 * It is easy to implement the logic. 
 * You can choose to enable or disable replicated subscription.
 * When you enable it, the overhead is low, and it is easy to configure. 
 * When you disable it, the overhead is zero.

### Limitations

When you enable replicated subscription, you're creating a consistent distributed snapshot to establish an association between message ids from different clusters. The snapshots are taken periodically. The default value is `1 second`. It means that a consumer failing over to a different cluster can potentially receive 1 second of duplicates. You can also configure the frequency of the snapshot in the `broker.conf` file.
>>>>>>> f773c602c... Test pr 10 (#27)
