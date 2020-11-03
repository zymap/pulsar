---
id: client-libraries-go
<<<<<<< HEAD
title: The Pulsar Go client
sidebar_label: Go
---

The Pulsar Go client can be used to create Pulsar [producers](#producers), [consumers](#consumers), and [readers](#readers) in Go (aka Golang).

> #### API docs available as well
> For standard API docs, consult the [Godoc](https://godoc.org/github.com/apache/pulsar/pulsar-client-go/pulsar).
=======
title: Pulsar Go client
sidebar_label: Go
---

> Tips: Currently, the CGo client will be deprecated, if you want to know more about the CGo client, please refer to [CGo client docs](client-libraries-cgo.md)

You can use Pulsar [Go client](https://github.com/apache/pulsar-client-go) to create Pulsar [producers](#producers), [consumers](#consumers), and [readers](#readers) in Go (aka Golang).

> #### API docs available as well
> For standard API docs, consult the [Godoc](https://godoc.org/github.com/apache/pulsar-client-go/pulsar).
>>>>>>> f773c602c... Test pr 10 (#27)


## Installation

<<<<<<< HEAD
### Requirements

Pulsar Go client library is based on the C++ client library. Follow
the instructions for [C++ library](client-libraries-cpp.md) for installing the binaries
through [RPM](client-libraries-cpp.md#rpm), [Deb](client-libraries-cpp.md#deb) or [Homebrew packages](client-libraries-cpp.md#macos).

### Installing go package

> #### Compatibility Warning
> The version number of the Go client **must match** the version number of the Pulsar C++ client library.

You can install the `pulsar` library locally using `go get`.  Note that `go get` doesn't support fetching a specific tag - it will always pull in master's version of the Go client.  You'll need a C++ client library that matches master.

```bash
$ go get -u github.com/apache/pulsar/pulsar-client-go/pulsar
```

Or you can use [dep](https://github.com/golang/dep) for managing the dependencies.

```bash
$ dep ensure -add github.com/apache/pulsar/pulsar-client-go/pulsar@v{{pulsar:version}}
=======
### Install go package

You can install the `pulsar` library locally using `go get`.  

```bash
$ go get -u "github.com/apache/pulsar-client-go/pulsar"
>>>>>>> f773c602c... Test pr 10 (#27)
```

Once installed locally, you can import it into your project:

```go
<<<<<<< HEAD
import "github.com/apache/pulsar/pulsar-client-go/pulsar"
=======
import "github.com/apache/pulsar-client-go/pulsar"
>>>>>>> f773c602c... Test pr 10 (#27)
```

## Connection URLs

To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol.md) URL.

Pulsar protocol URLs are assigned to specific clusters, use the `pulsar` scheme and have a default port of 6650. Here's an example for `localhost`:

```http
pulsar://localhost:6650
```

A URL for a production Pulsar cluster may look something like this:

```http
pulsar://pulsar.us-west.example.com:6650
```

If you're using [TLS](security-tls-authentication.md) authentication, the URL will look like something like this:

```http
pulsar+ssl://pulsar.us-west.example.com:6651
```

<<<<<<< HEAD
## Creating a client
=======
## Create a client
>>>>>>> f773c602c... Test pr 10 (#27)

In order to interact with Pulsar, you'll first need a `Client` object. You can create a client object using the `NewClient` function, passing in a `ClientOptions` object (more on configuration [below](#client-configuration)). Here's an example:


```go
import (
<<<<<<< HEAD
    "log"
    "runtime"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: "pulsar://localhost:6650",
        OperationTimeoutSeconds: 5,
        MessageListenerThreads: runtime.NumCPU(),
    })

    if err != nil {
        log.Fatalf("Could not instantiate Pulsar client: %v", err)
    }
=======
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()
>>>>>>> f773c602c... Test pr 10 (#27)
}
```

The following configurable parameters are available for Pulsar clients:

<<<<<<< HEAD
Parameter | Description | Default
:---------|:------------|:-------
`URL` | The connection URL for the Pulsar cluster. See [above](#urls) for more info |
`IOThreads` | The number of threads to use for handling connections to Pulsar [brokers](reference-terminology.md#broker) | 1
`OperationTimeoutSeconds` | The timeout for some Go client operations (creating producers, subscribing to and unsubscribing from [topics](reference-terminology.md#topic)). Retries will occur until this threshold is reached, at which point the operation will fail. | 30
`MessageListenerThreads` | The number of threads used by message listeners ([consumers](#consumers) and [readers](#readers)) | 1
`ConcurrentLookupRequests` | The number of concurrent lookup requests that can be sent on each broker connection. Setting a maximum helps to keep from overloading brokers. You should set values over the default of 5000 only if the client needs to produce and/or subscribe to thousands of Pulsar topics. | 5000
`Logger` | A custom logger implementation for the client (as a function that takes a log level, file path, line number, and message). All info, warn, and error messages will be routed to this function. | `nil`
`TLSTrustCertsFilePath` | The file path for the trusted TLS certificate |
`TLSAllowInsecureConnection` | Whether the client accepts untrusted TLS certificates from the broker | `false`
`Authentication` | Configure the authentication provider. (default: no authentication). Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")` | `nil`
`StatsIntervalInSeconds` | The interval (in seconds) at which client stats are published | 60
=======
 Name | Description | Default
| :-------- | :---------- |:---------- |
| URL | Configure the service URL for the Pulsar service. This parameter is required | |
| ConnectionTimeout | Timeout for the establishment of a TCP connection | 30s |
| OperationTimeout| Set the operation timeout. Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the operation will be marked as failed| 30s|
| Authentication | Configure the authentication provider. Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")` | no authentication |
| TLSTrustCertsFilePath | Set the path to the trusted TLS certificate file | |
| TLSAllowInsecureConnection | Configure whether the Pulsar client accept untrusted TLS certificate from broker | false |
| TLSValidateHostname | Configure whether the Pulsar client verify the validity of the host name from broker | false |
>>>>>>> f773c602c... Test pr 10 (#27)

## Producers

Pulsar producers publish messages to Pulsar topics. You can [configure](#producer-configuration) Go producers using a `ProducerOptions` object. Here's an example:

```go
producer, err := client.CreateProducer(pulsar.ProducerOptions{
<<<<<<< HEAD
    Topic: "my-topic",
})

if err != nil {
    log.Fatalf("Could not instantiate Pulsar producer: %v", err)
}

defer producer.Close()

msg := pulsar.ProducerMessage{
    Payload: []byte("Hello, Pulsar"),
}

if err := producer.Send(msg); err != nil {
    log.Fatalf("Producer could not send message: %v", err)
}
```

> #### Blocking operation
> When you create a new Pulsar producer, the operation will block (waiting on a go channel) until either a producer is successfully created or an error is thrown.


=======
	Topic: "my-topic",
})

_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
	Payload: []byte("hello"),
})

defer producer.Close()

if err != nil {
	fmt.Println("Failed to publish message", err)
}
fmt.Println("Published message")
```

>>>>>>> f773c602c... Test pr 10 (#27)
### Producer operations

Pulsar Go producers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Fetches the producer's [topic](reference-terminology.md#topic)| `string`
`Name()` | Fetches the producer's name | `string`
<<<<<<< HEAD
`Send(context.Context, ProducerMessage) error` | Publishes a [message](#messages) to the producer's topic. This call will block until the message is successfully acknowledged by the Pulsar broker, or an error will be thrown if the timeout set using the `SendTimeout` in the producer's [configuration](#producer-configuration) is exceeded. | `error`
`SendAsync(context.Context, ProducerMessage, func(ProducerMessage, error))` | Publishes a [message](#messages) to the producer's topic asynchronously. The third argument is a callback function that specifies what happens either when the message is acknowledged or an error is thrown. |
`Close()` | Closes the producer and releases all resources allocated to it. If `Close()` is called then no more messages will be accepted from the publisher. This method will block until all pending publish requests have been persisted by Pulsar. If an error is thrown, no pending writes will be retried. | `error`

Here's a more involved example usage of a producer:

```go
import (
    "context"
    "fmt"
    "log"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
    // Instantiate a Pulsar client
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: "pulsar://localhost:6650",
    })

    if err != nil { log.Fatal(err) }

    // Use the client to instantiate a producer
    producer, err := client.CreateProducer(pulsar.ProducerOptions{
        Topic: "my-topic",
    })

    if err != nil { log.Fatal(err) }

    ctx := context.Background()

    // Send 10 messages synchronously and 10 messages asynchronously
    for i := 0; i < 10; i++ {
        // Create a message
        msg := pulsar.ProducerMessage{
            Payload: []byte(fmt.Sprintf("message-%d", i)),
        }

        // Attempt to send the message
        if err := producer.Send(ctx, msg); err != nil {
            log.Fatal(err)
        }

        // Create a different message to send asynchronously
        asyncMsg := pulsar.ProducerMessage{
            Payload: []byte(fmt.Sprintf("async-message-%d", i)),
        }

        // Attempt to send the message asynchronously and handle the response
        producer.SendAsync(ctx, asyncMsg, func(msg pulsar.ProducerMessage, err error) {
            if err != nil { log.Fatal(err) }

            fmt.Printf("Message %s succesfully published", msg.ID())
        })
    }
}
```

### Producer configuration

Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar [topic](reference-terminology.md#topic) to which the producer will publish messages |
`Name` | A name for the producer. If you don't explicitly assign a name, Pulsar will automatically generate a globally unique name that you can access later using the `Name()` method.  If you choose to explicitly assign a name, it will need to be unique across *all* Pulsar clusters, otherwise the creation operation will throw an error. |
`SendTimeout` | When publishing a message to a topic, the producer will wait for an acknowledgment from the responsible Pulsar [broker](reference-terminology.md#broker). If a message is not acknowledged within the threshold set by this parameter, an error will be thrown. If you set `SendTimeout` to -1, the timeout will be set to infinity (and thus removed). Removing the send timeout is recommended when using Pulsar's [message de-duplication](cookbooks-deduplication.md) feature. | 30 seconds
`MaxPendingMessages` | The maximum size of the queue holding pending messages (i.e. messages waiting to receive an acknowledgment from the [broker](reference-terminology.md#broker)). By default, when the queue is full all calls to the `Send` and `SendAsync` methods will fail *unless* `BlockIfQueueFull` is set to `true`. |
`MaxPendingMessagesAcrossPartitions` | |
`BlockIfQueueFull` | If set to `true`, the producer's `Send` and `SendAsync` methods will block when the outgoing message queue is full rather than failing and throwing an error (the size of that queue is dictated by the `MaxPendingMessages` parameter); if set to `false` (the default), `Send` and `SendAsync` operations will fail and throw a `ProducerQueueIsFullError` when the queue is full. | `false`
`MessageRoutingMode` | The message routing logic (for producers on [partitioned topics](concepts-architecture-overview.md#partitioned-topics)). This logic is applied only when no key is set on messages. The available options are: round robin (`pulsar.RoundRobinDistribution`, the default), publishing all messages to a single partition (`pulsar.UseSinglePartition`), or a custom partitioning scheme (`pulsar.CustomPartition`). | `pulsar.RoundRobinDistribution`
`HashingScheme` | The hashing function that determines the partition on which a particular message is published (partitioned topics only). The available options are: `pulsar.JavaStringHash` (the equivalent of `String.hashCode()` in Java), `pulsar.Murmur3_32Hash` (applies the [Murmur3](https://en.wikipedia.org/wiki/MurmurHash) hashing function), or `pulsar.BoostHash` (applies the hashing function from C++'s [Boost](https://www.boost.org/doc/libs/1_62_0/doc/html/hash.html) library) | `pulsar.JavaStringHash`
`CompressionType` | The message data compression type used by the producer. The available options are [`LZ4`](https://github.com/lz4/lz4), [`ZLIB`](https://zlib.net/) and [`ZSTD`](https://facebook.github.io/zstd/). | No compression
`MessageRouter` | By default, Pulsar uses a round-robin routing scheme for [partitioned topics](cookbooks-partitioned.md). The `MessageRouter` parameter enables you to specify custom routing logic via a function that takes the Pulsar message and topic metadata as an argument and returns an integer (where the ), i.e. a function signature of `func(Message, TopicMetadata) int`. |
=======
`Send(context.Context, *ProducerMessage)` | Publishes a [message](#messages) to the producer's topic. This call will block until the message is successfully acknowledged by the Pulsar broker, or an error will be thrown if the timeout set using the `SendTimeout` in the producer's [configuration](#producer-configuration) is exceeded. | (MessageID, error)
`SendAsync(context.Context, *ProducerMessage, func(MessageID, *ProducerMessage, error))`| Send a message, this call will be blocking until is successfully acknowledged by the Pulsar broker. | 
`LastSequenceID()` | Get the last sequence id that was published by this producer. his represent either the automatically assigned or custom sequence id (set on the ProducerMessage) that was published and acknowledged by the broker. | int64
`Flush()`| Flush all the messages buffered in the client and wait until all messages have been successfully persisted. | error
`Close()` | Closes the producer and releases all resources allocated to it. If `Close()` is called then no more messages will be accepted from the publisher. This method will block until all pending publish requests have been persisted by Pulsar. If an error is thrown, no pending writes will be retried. | 

### Producer Example

#### How to use message router in producer

```go
client, err := NewClient(ClientOptions{
	URL: serviceURL,
})

if err != nil {
	log.Fatal(err)
}
defer client.Close()

// Only subscribe on the specific partition
consumer, err := client.Subscribe(ConsumerOptions{
	Topic:            "my-partitioned-topic-partition-2",
	SubscriptionName: "my-sub",
})

if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

producer, err := client.CreateProducer(ProducerOptions{
	Topic: "my-partitioned-topic",
	MessageRouter: func(msg *ProducerMessage, tm TopicMetadata) int {
		fmt.Println("Routing message ", msg, " -- Partitions: ", tm.NumPartitions())
		return 2
	},
})

if err != nil {
	log.Fatal(err)
}
defer producer.Close()
```

#### How to use delay relative in producer

```go
client, err := NewClient(ClientOptions{
	URL: "pulsar://localhost:6650",
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

topicName := newTopicName()
producer, err := client.CreateProducer(ProducerOptions{
	Topic: topicName,
})
if err != nil {
	log.Fatal(err)
}
defer producer.Close()

consumer, err := client.Subscribe(ConsumerOptions{
	Topic:            topicName,
	SubscriptionName: "subName",
	Type:             Shared,
})
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

ID, err := producer.Send(context.Background(), &ProducerMessage{
	Payload:      []byte(fmt.Sprintf("test")),
	DeliverAfter: 3 * time.Second,
})
if err != nil {
	log.Fatal(err)
}
fmt.Println(ID)

ctx, canc := context.WithTimeout(context.Background(), 1*time.Second)
msg, err := consumer.Receive(ctx)
if err != nil {
	log.Fatal(err)
}
fmt.Println(msg.Payload())
canc()

ctx, canc = context.WithTimeout(context.Background(), 5*time.Second)
msg, err = consumer.Receive(ctx)
if err != nil {
	log.Fatal(err)
}
fmt.Println(msg.Payload())
canc()
```


### Producer configuration

 Name | Description | Default
| :-------- | :---------- |:---------- |
| Topic | Topic specify the topic this consumer will subscribe to. This argument is required when constructing the reader. | |
| Name | Name specify a name for the producer. If not assigned, the system will generate a globally unique name which can be access with Producer.ProducerName(). | | 
| Properties | Properties attach a set of application defined properties to the producer This properties will be visible in the topic stats | |
| MaxPendingMessages| MaxPendingMessages set the max size of the queue holding the messages pending to receive an acknowledgment from the broker. | |
| HashingScheme | HashingScheme change the `HashingScheme` used to chose the partition on where to publish a particular message. | JavaStringHash |
| CompressionType | CompressionType set the compression type for the producer. | not compressed | 
| MessageRouter | MessageRouter set a custom message routing policy by passing an implementation of MessageRouter | |
| DisableBatching | DisableBatching control whether automatic batching of messages is enabled for the producer. | false |
| BatchingMaxPublishDelay | BatchingMaxPublishDelay set the time period within which the messages sent will be batched | 10ms |
| BatchingMaxMessages | BatchingMaxMessages set the maximum number of messages permitted in a batch. | 1000 | 
>>>>>>> f773c602c... Test pr 10 (#27)

## Consumers

Pulsar consumers subscribe to one or more Pulsar topics and listen for incoming messages produced on that topic/those topics. You can [configure](#consumer-configuration) Go consumers using a `ConsumerOptions` object. Here's a basic example that uses channels:

```go
<<<<<<< HEAD
msgChannel := make(chan pulsar.ConsumerMessage)

consumerOpts := pulsar.ConsumerOptions{
    Topic:            "my-topic",
    SubscriptionName: "my-subscription-1",
    Type:             pulsar.Exclusive,
    MessageChannel:   msgChannel,
}

consumer, err := client.Subscribe(consumerOpts)

if err != nil {
    log.Fatalf("Could not establish subscription: %v", err)
}

defer consumer.Close()

for cm := range msgChannel {
    msg := cm.Message

    fmt.Printf("Message ID: %s", msg.ID())
    fmt.Printf("Message value: %s", string(msg.Payload()))

    consumer.Ack(msg)
}
```

> #### Blocking operation
> When you create a new Pulsar consumer, the operation will block (on a go channel) until either a producer is successfully created or an error is thrown.

=======
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
	Topic:            "topic-1",
	SubscriptionName: "my-sub",
	Type:             pulsar.Shared,
})
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

for i := 0; i < 10; i++ {
	msg, err := consumer.Receive(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
		msg.ID(), string(msg.Payload()))

	consumer.Ack(msg)
}

if err := consumer.Unsubscribe(); err != nil {
	log.Fatal(err)
}
```
>>>>>>> f773c602c... Test pr 10 (#27)

### Consumer operations

Pulsar Go consumers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
<<<<<<< HEAD
`Topic()` | Returns the consumer's [topic](reference-terminology.md#topic) | `string`
`Subscription()` | Returns the consumer's subscription name | `string`
`Unsubcribe()` | Unsubscribes the consumer from the assigned topic. Throws an error if the unsubscribe operation is somehow unsuccessful. | `error`
`Receive(context.Context)` | Receives a single message from the topic. This method blocks until a message is available. | `(Message, error)`
`Ack(Message)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) | `error`
`AckID(MessageID)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) by message ID | `error`
`AckCumulative(Message)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) *all* the messages in the stream, up to and including the specified message. The `AckCumulative` method will block until the ack has been sent to the broker. After that, the messages will *not* be redelivered to the consumer. Cumulative acking can only be used with a [shared](concepts-messaging.md#shared) subscription type.
`Close()` | Closes the consumer, disabling its ability to receive messages from the broker | `error`
`RedeliverUnackedMessages()` | Redelivers *all* unacknowledged messages on the topic. In [failover](concepts-messaging.md#failover) mode, this request is ignored if the consumer isn't active on the specified topic; in [shared](concepts-messaging.md#shared) mode, redelivered messages are distributed across all consumers connected to the topic. **Note**: this is a *non-blocking* operation that doesn't throw an error. |

#### Receive example

Here's an example usage of a Go consumer that uses the `Receive()` method to process incoming messages:

```go
import (
    "context"
    "log"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
    // Instantiate a Pulsar client
    client, err := pulsar.NewClient(pulsar.ClientOptions{
            URL: "pulsar://localhost:6650",
    })

    if err != nil { log.Fatal(err) }

    // Use the client object to instantiate a consumer
    consumer, err := client.Subscribe(pulsar.ConsumerOptions{
        Topic:            "my-golang-topic",
        SubscriptionName: "sub-1",
        SubscriptionType: pulsar.Exclusive,
    })

    if err != nil { log.Fatal(err) }

    defer consumer.Close()

    ctx := context.Background()

    // Listen indefinitely on the topic
    for {
        msg, err := consumer.Receive(ctx)
        if err != nil { log.Fatal(err) }

        // Do something with the message

        consumer.Ack(msg)
    }
}
```

### Consumer configuration

Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar [topic](reference-terminology.md#topic) on which the consumer will establish a subscription and listen for messages |
`SubscriptionName` | The subscription name for this consumer |
`Name` | The name of the consumer |
`AckTimeout` | | 0
`SubscriptionType` | Available options are `Exclusive`, `Shared`, and `Failover` | `Exclusive`
`MessageChannel` | The Go channel used by the consumer. Messages that arrive from the Pulsar topic(s) will be passed to this channel. |
`ReceiverQueueSize` | Sets the size of the consumer's receiver queue, i.e. the number of messages that can be accumulated by the consumer before the application calls `Receive`. A value higher than the default of 1000 could increase consumer throughput, though at the expense of more memory utilization. | 1000
`MaxTotalReceiverQueueSizeAcrossPartitions` |Set the max total receiver queue size across partitions. This setting will be used to reduce the receiver queue size for individual partitions if the total exceeds this value | 50000
=======
`Subscription()` | Returns the consumer's subscription name | `string`
`Unsubcribe()` | Unsubscribes the consumer from the assigned topic. Throws an error if the unsubscribe operation is somehow unsuccessful. | `error`
`Receive(context.Context)` | Receives a single message from the topic. This method blocks until a message is available. | `(Message, error)`
`Ack(Message)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) | 
`AckID(MessageID)` | [Acknowledges](reference-terminology.md#acknowledgment-ack) a message to the Pulsar [broker](reference-terminology.md#broker) by message ID | 
`Nack(Message)` | Acknowledge the failure to process a single message. | 
`NackID(MessageID)` | Acknowledge the failure to process a single message. | 
`Seek(msgID MessageID)` | Reset the subscription associated with this consumer to a specific message id. The message id can either be a specific message or represent the first or last messages in the topic. | `error`
`SeekByTime(time time.Time)` | Reset the subscription associated with this consumer to a specific message publish time. | `error`
`Close()` | Closes the consumer, disabling its ability to receive messages from the broker | 

### Receive example

#### How to use regx consumer

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

defer client.Close()

p, err := client.CreateProducer(ProducerOptions{
	Topic:           topicInRegex,
	DisableBatching: true,
})
if err != nil {
	log.Fatal(err)
}
defer p.Close()

topicsPattern := fmt.Sprintf("persistent://%s/foo.*", namespace)
opts := ConsumerOptions{
	TopicsPattern:    topicsPattern,
	SubscriptionName: "regex-sub",
}
consumer, err := client.Subscribe(opts)
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()
```

#### How to use multi topics Consumer

```go
func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}


topic1 := "topic-1"
topic2 := "topic-2"

client, err := NewClient(ClientOptions{
	URL: "pulsar://localhost:6650",
})
if err != nil {
	log.Fatal(err)
}
topics := []string{topic1, topic2}
consumer, err := client.Subscribe(ConsumerOptions{
	Topics:           topics,
	SubscriptionName: "multi-topic-sub",
})
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()
```

#### How to use consumer listener

```go
import (
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	channel := make(chan pulsar.ConsumerMessage, 100)

	options := pulsar.ConsumerOptions{
		Topic:            "topic-1",
		SubscriptionName: "my-subscription",
		Type:             pulsar.Shared,
	}

	options.MessageChannel = channel

	consumer, err := client.Subscribe(options)
	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	for cm := range channel {
		msg := cm.Message
		fmt.Printf("Received message  msgId: %v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}
}
```

#### How to use consumer receive timeout

```go
client, err := NewClient(ClientOptions{
	URL: "pulsar://localhost:6650",
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

topic := "test-topic-with-no-messages"
ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
defer cancel()

// create consumer
consumer, err := client.Subscribe(ConsumerOptions{
	Topic:            topic,
	SubscriptionName: "my-sub1",
	Type:             Shared,
})
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

msg, err := consumer.Receive(ctx)
fmt.Println(msg.Payload())
if err != nil {
	log.Fatal(err)
}
```


### Consumer configuration

 Name | Description | Default
| :-------- | :---------- |:---------- |
| Topic | Topic specify the topic this consumer will subscribe to. This argument is required when constructing the reader. | |
| Topics | Specify a list of topics this consumer will subscribe on. Either a topic, a list of topics or a topics pattern are required when subscribing| |
| TopicsPattern | Specify a regular expression to subscribe to multiple topics under the same namespace. Either a topic, a list of topics or a topics pattern are required when subscribing | |
| AutoDiscoveryPeriod | Specify the interval in which to poll for new partitions or new topics if using a TopicsPattern. | |
| SubscriptionName | Specify the subscription name for this consumer. This argument is required when subscribing | |
| Name | Set the consumer name | | 
| Properties | Properties attach a set of application defined properties to the producer This properties will be visible in the topic stats | |
| Type | Select the subscription type to be used when subscribing to the topic. | Exclusive |
| SubscriptionInitialPosition | InitialPosition at which the cursor will be set when subscribe | Latest |
| DLQ | Configuration for Dead Letter Queue consumer policy. | no DLQ | 
| MessageChannel | Sets a `MessageChannel` for the consumer. When a message is received, it will be pushed to the channel for consumption | | 
| ReceiverQueueSize | Sets the size of the consumer receive queue. | 1000| 
| NackRedeliveryDelay | The delay after which to redeliver the messages that failed to be processed | 1min |
| ReadCompacted | If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog of the topic | false |
| ReplicateSubscriptionState | Mark the subscription as replicated to keep it in sync across clusters | false |
>>>>>>> f773c602c... Test pr 10 (#27)

## Readers

Pulsar readers process messages from Pulsar topics. Readers are different from consumers because with readers you need to explicitly specify which message in the stream you want to begin with (consumers, on the other hand, automatically begin with the most recent unacked message). You can [configure](#reader-configuration) Go readers using a `ReaderOptions` object. Here's an example:

```go
reader, err := client.CreateReader(pulsar.ReaderOptions{
<<<<<<< HEAD
    Topic: "my-golang-topic",
    StartMessageId: pulsar.LatestMessage,
})
```

> #### Blocking operation
> When you create a new Pulsar reader, the operation will block (on a go channel) until either a reader is successfully created or an error is thrown.


=======
	Topic:          "topic-1",
	StartMessageID: pulsar.EarliestMessageID(),
})
if err != nil {
	log.Fatal(err)
}
defer reader.Close()
```

>>>>>>> f773c602c... Test pr 10 (#27)
### Reader operations

Pulsar Go readers have the following methods available:

Method | Description | Return type
:------|:------------|:-----------
`Topic()` | Returns the reader's [topic](reference-terminology.md#topic) | `string`
`Next(context.Context)` | Receives the next message on the topic (analogous to the `Receive` method for [consumers](#consumer-operations)). This method blocks until a message is available. | `(Message, error)`
<<<<<<< HEAD
`Close()` | Closes the reader, disabling its ability to receive messages from the broker | `error`

#### "Next" example
=======
`HasNext()` | Check if there is any message available to read from the current position| (bool, error)
`Close()` | Closes the reader, disabling its ability to receive messages from the broker | `error`

### Reader example

#### How to use reader to read 'next' message
>>>>>>> f773c602c... Test pr 10 (#27)

Here's an example usage of a Go reader that uses the `Next()` method to process incoming messages:

```go
import (
<<<<<<< HEAD
    "context"
    "log"

    "github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
    // Instantiate a Pulsar client
    client, err := pulsar.NewClient(pulsar.ClientOptions{
            URL: "pulsar://localhost:6650",
    })

    if err != nil { log.Fatalf("Could not create client: %v", err) }

    // Use the client to instantiate a reader
    reader, err := client.CreateReader(pulsar.ReaderOptions{
        Topic:          "my-golang-topic",
        StartMessageID: pulsar.EarliestMessage,
    })

    if err != nil { log.Fatalf("Could not create reader: %v", err) }

    defer reader.Close()

    ctx := context.Background()

    // Listen on the topic for incoming messages
    for {
        msg, err := reader.Next(ctx)
        if err != nil { log.Fatalf("Error reading from topic: %v", err) }

        // Process the message
    }
}
```

In the example above, the reader begins reading from the earliest available message (specified by `pulsar.EarliestMessage`). The reader can also begin reading from the latest message (`pulsar.LatestMessage`) or some other message ID specified by bytes using the `DeserializeMessageID` function, which takes a byte array and returns a `MessageID` object. Here's an example:

```go
lastSavedId := // Read last saved message id from external store as byte[]

reader, err := client.CreateReader(pulsar.ReaderOptions{
    Topic:          "my-golang-topic",
    StartMessageID: DeserializeMessageID(lastSavedId),
})
=======
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          "topic-1",
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))
	}
}
```

In the example above, the reader begins reading from the earliest available message (specified by `pulsar.EarliestMessage`). The reader can also begin reading from the latest message (`pulsar.LatestMessage`) or some other message ID specified by bytes using the `DeserializeMessageID` function, which takes a byte array and returns a `MessageID` object. Here's an example:

```go
lastSavedId := // Read last saved message id from external store as byte[]

reader, err := client.CreateReader(pulsar.ReaderOptions{
    Topic:          "my-golang-topic",
    StartMessageID: pulsar.DeserializeMessageID(lastSavedId),
})
```

#### How to use reader to read specific message

```go
client, err := NewClient(ClientOptions{
	URL: lookupURL,
})

if err != nil {
	log.Fatal(err)
}
defer client.Close()

topic := "topic-1"
ctx := context.Background()

// create producer
producer, err := client.CreateProducer(ProducerOptions{
	Topic:           topic,
	DisableBatching: true,
})
if err != nil {
	log.Fatal(err)
}
defer producer.Close()

// send 10 messages
msgIDs := [10]MessageID{}
for i := 0; i < 10; i++ {
	msgID, err := producer.Send(ctx, &ProducerMessage{
		Payload: []byte(fmt.Sprintf("hello-%d", i)),
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)
	msgIDs[i] = msgID
}

// create reader on 5th message (not included)
reader, err := client.CreateReader(ReaderOptions{
	Topic:          topic,
	StartMessageID: msgIDs[4],
})

if err != nil {
	log.Fatal(err)
}
defer reader.Close()

// receive the remaining 5 messages
for i := 5; i < 10; i++ {
	msg, err := reader.Next(context.Background())
	if err != nil {
	log.Fatal(err)
}

// create reader on 5th message (included)
readerInclusive, err := client.CreateReader(ReaderOptions{
	Topic:                   topic,
	StartMessageID:          msgIDs[4],
	StartMessageIDInclusive: true,
})

if err != nil {
	log.Fatal(err)
}
defer readerInclusive.Close()
>>>>>>> f773c602c... Test pr 10 (#27)
```

### Reader configuration

<<<<<<< HEAD
Parameter | Description | Default
:---------|:------------|:-------
`Topic` | The Pulsar [topic](reference-terminology.md#topic) on which the reader will establish a subscription and listen for messages |
`Name` | The name of the reader |
`StartMessageID` | THe initial reader position, i.e. the message at which the reader begins processing messages. The options are `pulsar.EarliestMessage` (the earliest available message on the topic), `pulsar.LatestMessage` (the latest available message on the topic), or a `MessageID` object for a position that isn't earliest or latest. |
`MessageChannel` | The Go channel used by the reader. Messages that arrive from the Pulsar topic(s) will be passed to this channel. |
`ReceiverQueueSize` | Sets the size of the reader's receiver queue, i.e. the number of messages that can be accumulated by the reader before the application calls `Next`. A value higher than the default of 1000 could increase reader throughput, though at the expense of more memory utilization. | 1000
`SubscriptionRolePrefix` | The subscription role prefix. | `reader`
=======
 Name | Description | Default
| :-------- | :---------- |:---------- |
| Topic | Topic specify the topic this consumer will subscribe to. This argument is required when constructing the reader. | |
| Name | Name set the reader name. | | 
| Properties | Attach a set of application defined properties to the reader. This properties will be visible in the topic stats | |
| StartMessageID | StartMessageID initial reader positioning is done by specifying a message id. | |
| StartMessageIDInclusive | If true, the reader will start at the `StartMessageID`, included. Default is `false` and the reader will start from the "next" message | false |
| MessageChannel | MessageChannel sets a `MessageChannel` for the consumer When a message is received, it will be pushed to the channel for consumption| |
| ReceiverQueueSize | ReceiverQueueSize sets the size of the consumer receive queue. | 1000 |
| SubscriptionRolePrefix| SubscriptionRolePrefix set the subscription role prefix. | “reader” | 
| ReadCompacted | If enabled, the reader will read messages from the compacted topic rather than reading the full message backlog of the topic.  ReadCompacted can only be enabled when reading from a persistent topic. | false|
>>>>>>> f773c602c... Test pr 10 (#27)

## Messages

The Pulsar Go client provides a `ProducerMessage` interface that you can use to construct messages to producer on Pulsar topics. Here's an example message:

```go
msg := pulsar.ProducerMessage{
    Payload: []byte("Here is some message data"),
    Key: "message-key",
    Properties: map[string]string{
        "foo": "bar",
    },
    EventTime: time.Now(),
    ReplicationClusters: []string{"cluster1", "cluster3"},
}

<<<<<<< HEAD
if err := producer.send(msg); err != nil {
=======
if _, err := producer.send(msg); err != nil {
>>>>>>> f773c602c... Test pr 10 (#27)
    log.Fatalf("Could not publish message due to: %v", err)
}
```

The following methods parameters are available for `ProducerMessage` objects:

Parameter | Description
:---------|:-----------
`Payload` | The actual data payload of the message
`Key` | The optional key associated with the message (particularly useful for things like topic compaction)
`Properties` | A key-value map (both keys and values must be strings) for any application-specific metadata attached to the message
`EventTime` | The timestamp associated with the message
`ReplicationClusters` | The clusters to which this message will be replicated. Pulsar brokers handle message replication automatically; you should only change this setting if you want to override the broker default.
<<<<<<< HEAD
=======
`SequenceID` | Set the sequence id to assign to the current message
`DeliverAfter` | Request to deliver the message only after the specified relative delay
`DeliverAt` | Deliver the message only at or after the specified absolute timestamp
>>>>>>> f773c602c... Test pr 10 (#27)

## TLS encryption and authentication

In order to use [TLS encryption](security-tls-transport.md), you'll need to configure your client to do so:

 * Use `pulsar+ssl` URL type
 * Set `TLSTrustCertsFilePath` to the path to the TLS certs used by your client and the Pulsar broker
 * Configure `Authentication` option

Here's an example:

```go
opts := pulsar.ClientOptions{
    URL: "pulsar+ssl://my-cluster.com:6651",
    TLSTrustCertsFilePath: "/path/to/certs/my-cert.csr",
    Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem"),
}
```
<<<<<<< HEAD
=======

## OAuth2 authentication

To use [OAuth2 authentication](security-oauth2.md), you'll need to configure your client to perform the following operations.
This example shows how to configure OAuth2 authentication.

```go
oauth := pulsar.NewAuthenticationOAuth2(map[string]string{
		"type":       "client_credentials",
		"issuerUrl":  "https://dev-kt-aa9ne.us.auth0.com",
		"audience":   "https://dev-kt-aa9ne.us.auth0.com/api/v2/",
		"privateKey": "/path/to/privateKey",
		"clientId":   "0Xx...Yyxeny",
	})
client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:              "puslar://my-cluster:6650",
		Authentication:   oauth,
})
```
>>>>>>> f773c602c... Test pr 10 (#27)
