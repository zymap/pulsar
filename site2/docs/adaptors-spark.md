---
id: adaptors-spark
title: Pulsar adaptor for Apache Spark
sidebar_label: Apache Spark
---

The Spark Streaming receiver for Pulsar is a custom receiver that enables Apache [Spark Streaming](https://spark.apache.org/streaming/) to receive data from Pulsar.

An application can receive data in [Resilient Distributed Dataset](https://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds) (RDD) format via the Spark Streaming Pulsar receiver and can process it in a variety of ways.

## Prerequisites

To use the receiver, include a dependency for the `pulsar-spark` library in your Java configuration.

### Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{pulsar:version}}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-spark</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{pulsar:version}}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-spark', version: pulsarVersion
}
```

## Usage

Pass an instance of `SparkStreamingPulsarReceiver` to the `receiverStream` method in `JavaStreamingContext`:

```java
<<<<<<< HEAD
SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pulsar-spark");
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

ClientConfiguration clientConf = new ClientConfiguration();
ConsumerConfiguration consConf = new ConsumerConfiguration();
String url = "pulsar://localhost:6650/";
String topic = "persistent://public/default/topic1";
String subs = "sub1";

JavaReceiverInputDStream<byte[]> msgs = jssc
        .receiverStream(new SparkStreamingPulsarReceiver(clientConf, consConf, url, topic, subs));
=======
    String serviceUrl = "pulsar://localhost:6650/";
    String topic = "persistent://public/default/test_src";
    String subs = "test_sub";

    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Pulsar Spark Example");

    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

    ConsumerConfigurationData<byte[]> pulsarConf = new ConsumerConfigurationData();

    Set<String> set = new HashSet<>();
    set.add(topic);
    pulsarConf.setTopicNames(set);
    pulsarConf.setSubscriptionName(subs);

    SparkStreamingPulsarReceiver pulsarReceiver = new SparkStreamingPulsarReceiver(
        serviceUrl,
        pulsarConf,
        new AuthenticationDisabled());

    JavaReceiverInputDStream<byte[]> lineDStream = jsc.receiverStream(pulsarReceiver);
>>>>>>> f773c602c... Test pr 10 (#27)
```


## Example

<<<<<<< HEAD
You can find a complete example [here](https://github.com/apache/pulsar/tree/master/tests/pulsar-spark-test/src/test/java/org/apache/pulsar/spark/example/SparkStreamingPulsarReceiverExample.java).
=======
You can find a complete example [here](https://github.com/apache/pulsar/tree/master/examples/spark/src/main/java/org/apache/spark/streaming/receiver/example/SparkStreamingPulsarReceiverExample.java).
>>>>>>> f773c602c... Test pr 10 (#27)
In this example, the number of messages which contain the string "Pulsar" in received messages is counted.

