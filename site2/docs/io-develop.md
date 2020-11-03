---
id: io-develop
<<<<<<< HEAD
title: Develop Connectors
sidebar_label: Developing Connectors
---

This guide describes how developers can write new connectors for Pulsar IO to move data
between Pulsar and other systems. It describes how to create a Pulsar IO connector.

Pulsar IO connectors are specialized [Pulsar Functions](functions-overview.md). So writing
a Pulsar IO connector is as simple as writing a Pulsar function. Pulsar IO connectors come
in two flavors: {@inject: github:`Source`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java},
which import data from another system, and {@inject: github:`Sink`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java},
which export data to another system. For example, [KinesisSink](io-kinesis.md) would export
the messages of a Pulsar topic to a Kinesis stream, and [RabbitmqSource](io-rabbitmq.md) would import
the messages of a RabbitMQ queue to a Pulsar topic.

### Developing

#### Develop a source connector

What you need to develop a source connector is to implement {@inject: github:`Source`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java}
interface.

First, you need to implement the {@inject: github:`open`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java#L33} method. This method will be called once when the source connector
is initialized. In this method, you can retrieve all the connector specific settings through
the passed `config` parameter, and initialize all the necessary resourcess. For example, a Kafka
connector can create the Kafka client in this `open` method.

Beside the passed-in `config` object, the Pulsar runtime also provides a `SourceContext` for the
connector to access runtime resources for tasks like collecting metrics. The implementation can
save the `SourceContext` for futher usage.

```java
    /**
     * Open connector with configuration
     *
     * @param config initialization config
     * @param sourceContext
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(final Map<String, Object> config, SourceContext sourceContext) throws Exception;
```

The main task for a Source implementor is to implement {@inject: github:`read`:/master/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java#L41}
method.

```java
    /**
     * Reads the next message from source.
     * If source does not have any new messages, this call should block.
     * @return next message from source.  The return result should never be null
     * @throws Exception
     */
    Record<T> read() throws Exception;
```

The implementation should be blocking on this method if nothing to return. It should never return
`null`. The returned {@inject: github:`Record`:/master/pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java#L28} should encapsulates the information that is needed by
Pulsar IO runtime.

These information includes:

- *Topic Name*: _Optional_. If the record is originated from a Pulsar topic, it should be the Pulsar topic name.
- *Key*: _Optional_. If the record has a key associated with it.
- *Value*: _Required_. The actual data of this record.
- *Partition Id*: _Optional_. If the record is originated from a partitioned source,
  return its partition id. The partition id will be used as part of the unique identifier
  by Pulsar IO runtime to do message deduplication and achieve exactly-once processing guarantee.
- *Record Sequence*: _Optional_. If the record is originated from a sequential source,
  return its record sequence. The record sequence will be used as part of the unique identifier
  by Pulsar IO runtime to do message deduplication and achieve exactly-once processing guarantee.
- *Properties*: _Optional_. If the record carries user-defined properties, return those properties.

Additionally, the implemention of the record should provide two methods: `ack` and `fail`. These
two methods will be used by Pulsar IO connector to acknowledge the records that it has done
processing and fail the records that it has failed to process.

{@inject: github:`KafkaSource`:/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java} is a good example to follow.

#### Develop a sink connector

Developing a sink connector is as easy as developing a source connector. You just need to
implement {@inject: github:`Sink`:/master/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java} interface.

Similarly, you first need to implement the {@inject: github:`open`:/master/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java#L36} method to initialize all the necessary resources
before implementing the {@inject: github:`write`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java#L44} method.

```java
    /**
     * Open connector with configuration
     *
     * @param config initialization config
     * @param sinkContext
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(final Map<String, Object> config, SinkContext sinkContext) throws Exception;
```

The main task for a Sink implementor is to implement {@inject: github:`write`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java#L44} method.

```java
    /**
     * Write a message to Sink
     * @param inputRecordContext Context of input record from the source
     * @param record record to write to sink
     * @throws Exception
     */
    void write(Record<T> record) throws Exception;
```

In the implemention of `write` method, the implementor can decide how to write the value and
the optional key to the actual source, and leverage all the provided information such as
`Partition Id`, `Record Sequence` for achieving different processing guarantees. The implementor
is also responsible for acknowledging records if it has successfully written them or failing
records if has failed to write them.

### Testing

Testing connectors can be challenging because Pulsar IO connectors interact with two systems
that may be difficult to mock - Pulsar and the system the connector is connecting to. It is
recommended to write very specificially test the functionalities of the connector classes
while mocking the external services.

Once you have written sufficient unit tests for your connector, we also recommend adding
separate integration tests to verify end-to-end functionality. In Pulsar, we are using
[testcontainers](https://www.testcontainers.org/) for all Pulsar integration tests. Pulsar IO
{@inject: github:`IntegrationTests`:/tests/integration/src/test/java/org/apache/pulsar/tests/integration/io} are good examples to follow on integration testing your connectors.

### Packaging

Once you've developed and tested your connector, you must package it so that it can be submitted
to a [Pulsar Functions](functions-overview.md) cluster. There are two approaches described
here work with Pulsar Functions' runtime.

If you plan to package and distribute your connector for others to use, you are obligated to
properly license and copyright your own code and to adhere to the licensing and copyrights of
all libraries your code uses and that you include in your distribution. If you are using the
approach described in ["Creating a NAR package"](#creating-a-nar-package), the NAR plugin will
automatically create a `DEPENDENCIES` file in the generated NAR package, including the proper
licensing and copyrights of all libraries of your connector.

#### Creating a NAR package

The easiest approach to packaging a Pulsar IO connector is to create a NAR package using
[nifi-nar-maven-plugin](https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin).

NAR stands for NiFi Archive. It is a custom packaging mechanism used by Apache NiFi, to provide
a bit of Java ClassLoader isolation. For more details, you can read this
[blog post](https://medium.com/hashmapinc/nifi-nar-files-explained-14113f7796fd) to understand
how NAR works. Pulsar uses the same mechanism for packaging all the [builtin connectors](io-connectors).

All what you need is to include this [nifi-nar-maven-plugin](https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin) in your maven project for your connector. For example:
=======
title: How to develop Pulsar connectors
sidebar_label: Develop
---

This guide describes how to develop Pulsar connectors to move data
between Pulsar and other systems. 

Pulsar connectors are special [Pulsar Functions](functions-overview.md), so creating
a Pulsar connector is similar to creating a Pulsar function. 

Pulsar connectors come in two types: 

| Type | Description | Example
|---|---|---
{@inject: github:`Source`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java}|Import data from another system to Pulsar.|[RabbitMQ source connector](io-rabbitmq.md) imports the messages of a RabbitMQ queue to a Pulsar topic.
{@inject: github:`Sink`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java}|Export data from Pulsar to another system.|[Kinesis sink connector](io-kinesis.md) exports the messages of a Pulsar topic to a Kinesis stream.

## Develop

You can develop Pulsar source connectors and sink connectors.

### Source

Developing a source connector is to implement the {@inject: github:`Source`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java}
interface, which means you need to implement the {@inject: github:`open`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java} method and the {@inject: github:`read`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java} method.

1. Implement the {@inject: github:`open`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java} method. 

    ```java
    /**
    * Open connector with configuration
    *
    * @param config initialization config
    * @param sourceContext
    * @throws Exception IO type exceptions when opening a connector
    */
    void open(final Map<String, Object> config, SourceContext sourceContext) throws Exception;
    ```

    This method is called when the source connector is initialized. 

    In this method, you can retrieve all connector specific settings through the passed-in `config` parameter and initialize all necessary resources. 
    
    For example, a Kafka connector can create a Kafka client in this `open` method.

    Besides, Pulsar runtime also provides a `SourceContext` for the 
    connector to access runtime resources for tasks like collecting metrics. The implementation can save the `SourceContext` for future use.

2. Implement the {@inject: github:`read`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Source.java} method.

    ```java
        /**
        * Reads the next message from source.
        * If source does not have any new messages, this call should block.
        * @return next message from source.  The return result should never be null
        * @throws Exception
        */
        Record<T> read() throws Exception;
    ```

    If nothing to return, the implementation should be blocking rather than returning `null`. 

    The returned {@inject: github:`Record`:/pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java} should encapsulate the following information, which is needed by Pulsar IO runtime. 

    * {@inject: github:`Record`:/pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java} should provide the following variables:

      |Variable|Required|Description
      |---|---|---
      `TopicName`|No|Pulsar topic name from which the record is originated from.
      `Key`|No| Messages can optionally be tagged with keys.<br/><br/>For more information, see [Routing modes](concepts-messaging.md#routing-modes).|
      `Value`|Yes|Actual data of the record.
      `EventTime`|No|Event time of the record from the source.
      `PartitionId`|No| If the record is originated from a partitioned source, it returns its `PartitionId`. <br/><br/>`PartitionId` is used as a part of the unique identifier by Pulsar IO runtime to deduplicate messages and achieve exactly-once processing guarantee.
      `RecordSequence`|No|If the record is originated from a sequential source, it returns its `RecordSequence`.<br/><br/>`RecordSequence` is used as a part of the unique identifier by Pulsar IO runtime to deduplicate messages and achieve exactly-once processing guarantee.
      `Properties` |No| If the record carries user-defined properties, it returns those properties.
      `DestinationTopic`|No|Topic to which message should be written.
      `Message`|No|A class which carries data sent by users.<br/><br/>For more information, see [Message.java](https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/Message.java).|

     * {@inject: github:`Record`:/pulsar-functions/api-java/src/main/java/org/apache/pulsar/functions/api/Record.java} should provide the following methods:

        Method|Description
        |---|---
        `ack` |Acknowledge that the record is fully processed.
        `fail`|Indicate that the record fails to be processed.

> #### Tip
>
> For more information about **how to create a source connector**, see {@inject: github:`KafkaSource`:/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java}.

### Sink

Developing a sink connector **is similar to** developing a source connector, that is, you need to implement the {@inject: github:`Sink`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java} interface, which means implementing the {@inject: github:`open`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java} method and the {@inject: github:`write`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java} method.

1. Implement the {@inject: github:`open`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java} method.

    ```java
        /**
        * Open connector with configuration
        *
        * @param config initialization config
        * @param sinkContext
        * @throws Exception IO type exceptions when opening a connector
        */
        void open(final Map<String, Object> config, SinkContext sinkContext) throws Exception;
    ```

2. Implement the {@inject: github:`write`:/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/Sink.java} method.

    ```java
        /**
        * Write a message to Sink
        * @param record record to write to sink
        * @throws Exception
        */
        void write(Record<T> record) throws Exception;
    ```

    During the implementation, you can decide how to write the `Value` and
    the `Key` to the actual source, and leverage all the provided information such as
    `PartitionId` and `RecordSequence` to achieve different processing guarantees. 

    You also need to ack records (if messages are sent successfully) or fail records (if messages fail to send). 

## Test

Testing connectors can be challenging because Pulsar IO connectors interact with two systems
that may be difficult to mock—Pulsar and the system to which the connector is connecting. 

It is
recommended writing special tests to test the connector functionalities as below
while mocking the external service. 

### Unit test

You can create unit tests for your connector.

### Integration test

Once you have written sufficient unit tests, you can add
separate integration tests to verify end-to-end functionality. 

Pulsar uses
[testcontainers](https://www.testcontainers.org/) **for all integration tests**. 

> #### Tip
>
>For more information about **how to create integration tests for Pulsar connectors**, see {@inject: github:`IntegrationTests`:/tests/integration/src/test/java/org/apache/pulsar/tests/integration/io}.

## Package

Once you've developed and tested your connector, you need to package it so that it can be submitted
to a [Pulsar Functions](functions-overview.md) cluster. 

There are two methods to
work with Pulsar Functions' runtime, that is, [NAR](#nar) and [uber JAR](#uber-jar).

> #### Note
> 
> If you plan to package and distribute your connector for others to use, you are obligated to
license and copyright your own code properly. Remember to add the license and copyright to
all libraries your code uses and to your distribution. 
>
> If you use the [NAR](#nar) method, the NAR plugin 
automatically creates a `DEPENDENCIES` file in the generated NAR package, including the proper
licensing and copyrights of all libraries of your connector.

### NAR 

**NAR** stands for NiFi Archive, which is a custom packaging mechanism used by Apache NiFi, to provide
a bit of Java ClassLoader isolation. 

> #### Tip
> 
> For more information about **how NAR works**, see
> [here](https://medium.com/hashmapinc/nifi-nar-files-explained-14113f7796fd). 

Pulsar uses the same mechanism for packaging **all** [built-in connectors](io-connectors). 

The easiest approach to package a Pulsar connector is to create a NAR package using
[nifi-nar-maven-plugin](https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin).

Include this [nifi-nar-maven-plugin](https://mvnrepository.com/artifact/org.apache.nifi/nifi-nar-maven-plugin) in your maven project for your connector as below. 
>>>>>>> f773c602c... Test pr 10 (#27)

```xml
<plugins>
  <plugin>
    <groupId>org.apache.nifi</groupId>
    <artifactId>nifi-nar-maven-plugin</artifactId>
    <version>1.2.0</version>
  </plugin>
</plugins>
```

<<<<<<< HEAD
The {@inject: github:`TwitterFirehose`:/pulsar-io/twitter} connector is a good example to follow.

#### Creating an Uber JAR

An alternative approach is to create an _uber JAR_ that contains all of the connector's JAR files
and other resource files. No directory internal structure is necessary.

You can use [maven-shade-plugin](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html) to create a Uber JAR. For example:
=======
You must also create a `resources/META-INF/services/pulsar-io.yaml` file with the following contents:

```yaml
name: connector name
description: connector description
sourceClass: fully qualified class name (only if source connector)
sinkClass: fully qualified class name (only if sink connector)
```

If you are using the [Gradle NiFi plugin](https://github.com/sponiro/gradle-nar-plugin) you might need to create a directive to ensure your pulsar-io.yaml is [copied into the NAR file correctly](https://github.com/sponiro/gradle-nar-plugin/issues/5).

> #### Tip
> 
> For more information about an **how to use NAR for Pulsar connectors**, see {@inject: github:`TwitterFirehose`:/pulsar-io/twitter/pom.xml}.

### Uber JAR

An alternative approach is to create an **uber JAR** that contains all of the connector's JAR files
and other resource files. No directory internal structure is necessary.

You can use [maven-shade-plugin](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html) to create a uber JAR as below:
>>>>>>> f773c602c... Test pr 10 (#27)

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <version>3.1.1</version>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
<<<<<<< HEAD
    </execution>
    <configuration>
      <filters>
        <filter>
          <artifact>*:*</artifact>
        </filter>
      </filters>
    </configuration>
=======
      <configuration>
        <filters>
          <filter>
            <artifact>*:*</artifact>
          </filter>
        </filters>
      </configuration>
    </execution>
>>>>>>> f773c602c... Test pr 10 (#27)
  </executions>
</plugin>
```
