---
id: sql-overview
title: Pulsar SQL Overview
sidebar_label: Overview
---

<<<<<<< HEAD
One of the common use cases of Pulsar is storing streams of event data. Often the event data is structured which predefined fields.  There is tremendous value for users to be able to query the existing data that is already stored in Pulsar topics.  With the implementation of the [Schema Registry](concepts-schema-registry.md), structured data can be stored in Pulsar and allows for the potential to query that data via SQL language.

By leveraging [Presto](https://prestosql.io/), we have created a method for users to be able to query structured data stored within Pulsar in a very efficient and scalable manner. We will discuss why this very efficient and scalable in the [Performance](#performance) section below. 

At the core of this Pulsar SQL is the Presto Pulsar connector which allows Presto workers within a Presto cluster to query data from Pulsar.


![The Pulsar consumer and reader interfaces](assets/pulsar-sql-arch-2.png)


## Performance

The reason why query performance is very efficient and highly scalable because of Pulsar's [two level segment based architecture](concepts-architecture-overview.md#apache-bookkeeper). 

Topics in Pulsar are stored as segments in [Apache Bookkeeper](https://bookkeeper.apache.org/). Each topic segment is also replicated to a configurable (default 3) number of Bookkeeper nodes which allows for concurrent reads and high read throughput. In the Presto Pulsar connector, we read data directly from Bookkeeper to take advantage of the Pulsar's segment based architecture.  Thus, Presto workers can read concurrently from horizontally scalable number bookkeeper nodes.


![The Pulsar consumer and reader interfaces](assets/pulsar-sql-arch-1.png)
=======
Apache Pulsar is used to store streams of event data, and the event data is structured with predefined fields. With the implementation of the [Schema Registry](schema-get-started.md), you can store structured data in Pulsar and query the data by using [Presto](https://prestosql.io/).  

As the core of Pulsar SQL, Presto Pulsar connector enables Presto workers within a Presto cluster to query data from Pulsar.

![The Pulsar consumer and reader interfaces](assets/pulsar-sql-arch-2.png)

The query performance is efficient and highly scalable, because Pulsar adopts [two level segment based architecture](concepts-architecture-overview.md#apache-bookkeeper). 

Topics in Pulsar are stored as segments in [Apache BookKeeper](https://bookkeeper.apache.org/). Each topic segment is replicated to some BookKeeper nodes, which enables concurrent reads and high read throughput. You can configure the number of BookKeeper nodes, and the default number is `3`. In Presto Pulsar connector, data is read directly from BookKeeper, so Presto workers can read concurrently from horizontally scalable number BookKeeper nodes.

![The Pulsar consumer and reader interfaces](assets/pulsar-sql-arch-1.png)
>>>>>>> f773c602c... Test pr 10 (#27)
