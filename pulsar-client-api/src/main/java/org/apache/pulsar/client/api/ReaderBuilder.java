/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
<<<<<<< HEAD
=======
import java.util.concurrent.TimeUnit;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * {@link ReaderBuilder} is used to configure and create instances of {@link Reader}.
 *
 * @see PulsarClient#newReader()
 *
 * @since 2.0.0
 */
public interface ReaderBuilder<T> extends Cloneable {

    /**
     * Finalize the creation of the {@link Reader} instance.
<<<<<<< HEAD
     * <p>
     * This method will block until the reader is created successfully or an exception is thrown.
=======
     *
     * <p>This method will block until the reader is created successfully or an exception is thrown.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @return the reader instance
     * @throws PulsarClientException
     *             if the reader creation fails
     */
    Reader<T> create() throws PulsarClientException;

    /**
     * Finalize the creation of the {@link Reader} instance in asynchronous mode.
     *
<<<<<<< HEAD
     * <p>
     * This method will return a {@link CompletableFuture} that can be used to access the instance when it's ready.
=======
     * <p>This method will return a {@link CompletableFuture} that can be used to access the instance when it's ready.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @return the reader instance
     * @throws PulsarClientException
     *             if the reader creation fails
     */
    CompletableFuture<Reader<T>> createAsync();

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
<<<<<<< HEAD
     * <p>
     * Example:
=======
     * <p>Example:
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * <pre>{@code
     * Map<String, Object> config = new HashMap<>();
     * config.put("topicName", "test-topic");
     * config.put("receiverQueueSize", 2000);
     *
     * ReaderBuilder<byte[]> builder = ...;
     * builder = builder.loadConf(config);
     *
     * Reader<byte[]> reader = builder.create();
     * }</pre>
     *
     * @param config
     *            configuration to load
     * @return the reader builder instance
     */
    ReaderBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Create a copy of the current {@link ReaderBuilder}.
<<<<<<< HEAD
     * <p>
     * Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
=======
     *
     * <p>Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
>>>>>>> f773c602c... Test pr 10 (#27)
     * example:
     *
     * <pre>{@code
     * ReaderBuilder<String> builder = client.newReader(Schema.STRING)
     *             .readerName("my-reader")
     *             .receiverQueueSize(10);
     *
     * Reader<String> reader1 = builder.clone().topic("topic-1").create();
     * Reader<String> reader2 = builder.clone().topic("topic-2").create();
     * }</pre>
     *
     * @return a clone of the reader builder instance
     */
    ReaderBuilder<T> clone();

    /**
     * Specify the topic this reader will read from.
<<<<<<< HEAD
     * <p>
     * This argument is required when constructing the reader.
=======
     *
     * <p>This argument is required when constructing the reader.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param topicName
     *            the name of the topic
     * @return the reader builder instance
     */
    ReaderBuilder<T> topic(String topicName);

    /**
     * The initial reader positioning is done by specifying a message id. The options are:
     * <ul>
     * <li>{@link MessageId#earliest}: Start reading from the earliest message available in the topic</li>
     * <li>{@link MessageId#latest}: Start reading from end of the topic. The first message read will be the one
     * published <b>*after*</b> the creation of the builder</li>
     * <li>{@link MessageId}: Position the reader on a particular message. The first message read will be the one
     * immediately <b>*after*</b> the specified message</li>
     * </ul>
     *
<<<<<<< HEAD
=======
     * <p>If the first message <b>*after*</b> the specified message is not the desired behaviour, use
     * {@link ReaderBuilder#startMessageIdInclusive()}.
     *
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param startMessageId the message id where the reader will be initially positioned on
     * @return the reader builder instance
     */
    ReaderBuilder<T> startMessageId(MessageId startMessageId);

    /**
<<<<<<< HEAD
     * Sets a {@link ReaderListener} for the reader
     * <p>
     * When a {@link ReaderListener} is set, application will receive messages through it. Calls to
=======
     * The initial reader positioning can be set at specific timestamp by providing total rollback duration. so, broker
     * can find a latest message that was published before given duration. <br/>
     * eg: rollbackDuration in minute = 5 suggests broker to find message which was published 5 mins back and set the
     * inital position on that messageId.
     *
     * @param rollbackDuration
     *            duration which position should be rolled back.
     * @return
     */
    ReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit);

    /**
     * Set the reader to include the given position of {@link ReaderBuilder#startMessageId(MessageId)}
     *
     * <p>This configuration option also applies for any cursor reset operation like {@link Reader#seek(MessageId)}.
     *
     * @return the reader builder instance
     */
    ReaderBuilder<T> startMessageIdInclusive();

    /**
     * Sets a {@link ReaderListener} for the reader.
     *
     * <p>When a {@link ReaderListener} is set, application will receive messages through it. Calls to
>>>>>>> f773c602c... Test pr 10 (#27)
     * {@link Reader#readNext()} will not be allowed.
     *
     * @param readerListener
     *            the listener object
     * @return the reader builder instance
     */
    ReaderBuilder<T> readerListener(ReaderListener<T> readerListener);

    /**
     * Sets a {@link CryptoKeyReader} to decrypt the message payloads.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     * @return the reader builder instance
     */
    ReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
<<<<<<< HEAD
     * Sets the {@link ConsumerCryptoFailureAction} to specify
=======
     * Sets the {@link ConsumerCryptoFailureAction} to specify.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param action
     *            The action to take when the decoding fails
     * @return the reader builder instance
     */
    ReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * Sets the size of the consumer receive queue.
<<<<<<< HEAD
     * <p>
     * The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     * <p>
     * Default value is {@code 1000} messages and should be good for most use cases.
=======
     *
     * <p>The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     *
     * <p>Default value is {@code 1000} messages and should be good for most use cases.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     * @return the reader builder instance
     */
    ReaderBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * Specify a reader name.
<<<<<<< HEAD
     * <p>
     * The reader name is purely informational and can used to track a particular reader in the reported stats. By
     * default a randomly generated name is used.
=======
     *
     * <p>The reader name is purely informational and can used to track a particular reader in the reported stats.
     * By default a randomly generated name is used.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param readerName
     *            the name to use for the reader
     * @return the reader builder instance
     */
    ReaderBuilder<T> readerName(String readerName);

    /**
     * Set the subscription role prefix. The default prefix is "reader".
     *
     * @param subscriptionRolePrefix
     * @return the reader builder instance
     */
    ReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix);

    /**
     * If enabled, the reader will read messages from the compacted topic rather than reading the full message backlog
     * of the topic. This means that, if the topic has been compacted, the reader will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages will be sent as normal.
<<<<<<< HEAD
     * <p>
     * readCompacted can only be enabled when reading from a persistent topic. Attempting to enable it on non-persistent
     * topics will lead to the reader create call throwing a {@link PulsarClientException}.
=======
     *
     * <p>readCompacted can only be enabled when reading from a persistent topic. Attempting to enable it
     * on non-persistent topics will lead to the reader create call throwing a {@link PulsarClientException}.
>>>>>>> f773c602c... Test pr 10 (#27)
     *
     * @param readCompacted
     *            whether to read from the compacted topic
     * @return the reader builder instance
     */
    ReaderBuilder<T> readCompacted(boolean readCompacted);
<<<<<<< HEAD
=======

    /**
     * Set key hash range of the reader, broker will only dispatch messages which hash of the message key contains by
     * the specified key hash range. Multiple key hash ranges can be specified on a reader.
     *
     * <p>Total hash range size is 65536, so the max end of the range should be less than or equal to 65535.
     *
     * @param ranges
     *            key hash ranges for a reader
     * @return the reader builder instance
     */
    ReaderBuilder<T> keyHashRange(Range... ranges);
>>>>>>> f773c602c... Test pr 10 (#27)
}
