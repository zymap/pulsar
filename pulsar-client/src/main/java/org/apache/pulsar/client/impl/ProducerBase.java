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
package org.apache.pulsar.client.impl;

<<<<<<< HEAD
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
=======
import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.CompletableFuture;
>>>>>>> f773c602c... Test pr 10 (#27)

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
<<<<<<< HEAD
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;
=======
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
>>>>>>> f773c602c... Test pr 10 (#27)

public abstract class ProducerBase<T> extends HandlerState implements Producer<T> {

    protected final CompletableFuture<Producer<T>> producerCreatedFuture;
    protected final ProducerConfigurationData conf;
    protected final Schema<T> schema;
<<<<<<< HEAD
    protected final ProducerInterceptors<T> interceptors;

    protected ProducerBase(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
            CompletableFuture<Producer<T>> producerCreatedFuture, Schema<T> schema, ProducerInterceptors<T> interceptors) {
=======
    protected final ProducerInterceptors interceptors;
    protected final ConcurrentOpenHashMap<SchemaHash, byte[]> schemaCache;
    protected volatile MultiSchemaMode multiSchemaMode = MultiSchemaMode.Auto;

    protected ProducerBase(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
            CompletableFuture<Producer<T>> producerCreatedFuture, Schema<T> schema, ProducerInterceptors interceptors) {
>>>>>>> f773c602c... Test pr 10 (#27)
        super(client, topic);
        this.producerCreatedFuture = producerCreatedFuture;
        this.conf = conf;
        this.schema = schema;
        this.interceptors = interceptors;
<<<<<<< HEAD
=======
        this.schemaCache = new ConcurrentOpenHashMap<>();
        if (!conf.isMultiSchema()) {
            multiSchemaMode = MultiSchemaMode.Disabled;
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public MessageId send(T message) throws PulsarClientException {
        return newMessage().value(message).send();
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(T message) {
        try {
            return newMessage().value(message).sendAsync();
        } catch (SchemaSerializationException e) {
            return FutureUtil.failedFuture(e);
        }
    }

<<<<<<< HEAD
    public CompletableFuture<MessageId> sendAsync(Message<T> message) {
        return internalSendAsync(message);
=======
    public CompletableFuture<MessageId> sendAsync(Message<?> message) {
        return internalSendAsync(message, null);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public TypedMessageBuilder<T> newMessage() {
        return new TypedMessageBuilderImpl<>(this, schema);
    }

<<<<<<< HEAD
    abstract CompletableFuture<MessageId> internalSendAsync(Message<T> message);

    public MessageId send(Message<T> message) throws PulsarClientException {
        try {
            // enqueue the message to the buffer
            CompletableFuture<MessageId> sendFuture = internalSendAsync(message);
=======
    public <V> TypedMessageBuilder<V> newMessage(Schema<V> schema) {
        checkArgument(schema != null);
        return new TypedMessageBuilderImpl<>(this, schema);
    }

    // TODO: add this method to the Producer interface
    // @Override
    public TypedMessageBuilder<T> newMessage(Transaction txn) {
        checkArgument(txn instanceof TransactionImpl);

        // check the producer has proper settings to send transactional messages
        if (conf.getSendTimeoutMs() > 0) {
            throw new IllegalArgumentException("Only producers disabled sendTimeout are allowed to"
                + " produce transactional messages");
        }

        return new TypedMessageBuilderImpl<>(this, schema, (TransactionImpl) txn);
    }

    abstract CompletableFuture<MessageId> internalSendAsync(Message<?> message, Transaction txn);

    public MessageId send(Message<?> message) throws PulsarClientException {
        try {
            // enqueue the message to the buffer
            CompletableFuture<MessageId> sendFuture = internalSendAsync(message, null);
>>>>>>> f773c602c... Test pr 10 (#27)

            if (!sendFuture.isDone()) {
                // the send request wasn't completed yet (e.g. not failing at enqueuing), then attempt to triggerFlush it out
                triggerFlush();
            }

            return sendFuture.get();
<<<<<<< HEAD
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
=======
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public void flush() throws PulsarClientException {
        try {
            flushAsync().get();
<<<<<<< HEAD
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PulsarClientException) {
                throw (PulsarClientException) cause;
            } else {
                throw new PulsarClientException(cause);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
=======
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    abstract void triggerFlush();

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
<<<<<<< HEAD
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
=======
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    abstract public CompletableFuture<Void> closeAsync();

    @Override
    public String getTopic() {
        return topic;
    }

    public ProducerConfigurationData getConfiguration() {
        return conf;
    }

    public CompletableFuture<Producer<T>> producerCreatedFuture() {
        return producerCreatedFuture;
    }

<<<<<<< HEAD
    protected Message<T> beforeSend(Message<T> message) {
=======
    protected Message<?> beforeSend(Message<?> message) {
>>>>>>> f773c602c... Test pr 10 (#27)
        if (interceptors != null) {
            return interceptors.beforeSend(this, message);
        } else {
            return message;
        }
    }

<<<<<<< HEAD
    protected void onSendAcknowledgement(Message<T> message, MessageId msgId, Throwable exception) {
=======
    protected void onSendAcknowledgement(Message<?> message, MessageId msgId, Throwable exception) {
>>>>>>> f773c602c... Test pr 10 (#27)
        if (interceptors != null) {
            interceptors.onSendAcknowledgement(this, message, msgId, exception);
        }
    }

    @Override
    public String toString() {
        return "ProducerBase{" + "topic='" + topic + '\'' + '}';
    }
<<<<<<< HEAD
=======

    public enum MultiSchemaMode {
        Auto, Enabled, Disabled
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
