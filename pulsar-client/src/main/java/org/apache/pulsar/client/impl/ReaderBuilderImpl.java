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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

=======
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.AccessLevel;
import lombok.Getter;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.Range;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;
<<<<<<< HEAD

=======
import static org.apache.pulsar.client.api.KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;

@Getter(AccessLevel.PUBLIC)
>>>>>>> f773c602c... Test pr 10 (#27)
public class ReaderBuilderImpl<T> implements ReaderBuilder<T> {

    private final PulsarClientImpl client;

    private ReaderConfigurationData<T> conf;

    private final Schema<T> schema;

<<<<<<< HEAD
    ReaderBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
=======
    public ReaderBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
>>>>>>> f773c602c... Test pr 10 (#27)
        this(client, new ReaderConfigurationData<T>(), schema);
    }

    private ReaderBuilderImpl(PulsarClientImpl client, ReaderConfigurationData<T> conf, Schema<T> schema) {
        this.client = client;
        this.conf = conf;
        this.schema = schema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ReaderBuilder<T> clone() {
<<<<<<< HEAD
        try {
            return (ReaderBuilder<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ReaderBuilderImpl");
        }
=======
        return new ReaderBuilderImpl<>(client, conf.clone(), schema);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public Reader<T> create() throws PulsarClientException {
        try {
            return createAsync().get();
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
    public CompletableFuture<Reader<T>> createAsync() {
        if (conf.getTopicName() == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the reader builder"));
        }

<<<<<<< HEAD
        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Start message id must be set on the reader builder"));
=======
        if (conf.getStartMessageId() != null && conf.getStartMessageFromRollbackDurationInSec() > 0 ||
                conf.getStartMessageId() == null && conf.getStartMessageFromRollbackDurationInSec() <= 0) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException(
                            "Start message id or start message from roll back must be specified but they cannot be specified at the same time"));
        }

        if (conf.getStartMessageFromRollbackDurationInSec() > 0) {
            conf.setStartMessageId(MessageId.earliest);
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        return client.createReaderAsync(conf, schema);
    }

    @Override
    public ReaderBuilder<T> loadConf(Map<String, Object> config) {
<<<<<<< HEAD
        conf = ConfigurationDataUtils.loadData(config, conf, ReaderConfigurationData.class);
=======
        MessageId startMessageId = conf.getStartMessageId();
        conf = ConfigurationDataUtils.loadData(config, conf, ReaderConfigurationData.class);
        conf.setStartMessageId(startMessageId);
>>>>>>> f773c602c... Test pr 10 (#27)
        return this;
    }

    @Override
    public ReaderBuilder<T> topic(String topicName) {
<<<<<<< HEAD
        conf.setTopicName(topicName);
=======
        conf.setTopicName(StringUtils.trim(topicName));
>>>>>>> f773c602c... Test pr 10 (#27)
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageId(MessageId startMessageId) {
        conf.setStartMessageId(startMessageId);
        return this;
    }

    @Override
<<<<<<< HEAD
=======
    public ReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit) {
        conf.setStartMessageFromRollbackDurationInSec(timeunit.toSeconds(rollbackDuration));
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageIdInclusive() {
        conf.setResetIncludeHead(true);
        return this;
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public ReaderBuilder<T> readerListener(ReaderListener<T> readerListener) {
        conf.setReaderListener(readerListener);
        return this;
    }

    @Override
    public ReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ReaderBuilder<T> receiverQueueSize(int receiverQueueSize) {
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public ReaderBuilder<T> readerName(String readerName) {
        conf.setReaderName(readerName);
        return this;
    }

    @Override
    public ReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix) {
        conf.setSubscriptionRolePrefix(subscriptionRolePrefix);
        return this;
    }

    @Override
    public ReaderBuilder<T> readCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }
<<<<<<< HEAD
=======

    @Override
    public ReaderBuilder<T> keyHashRange(Range... ranges) {
        Preconditions.checkArgument(ranges != null && ranges.length > 0,
                "Cannot specify a null ofr an empty key hash ranges for a reader");
        for (int i = 0; i < ranges.length; i++) {
            Range range1 = ranges[i];
            if (range1.getStart() < 0 || range1.getEnd() > DEFAULT_HASH_RANGE_SIZE) {
                throw new IllegalArgumentException("Ranges must be [0, 65535] but provided range is " + range1);
            }
            for (int j = 0; j < ranges.length; j++) {
                Range range2 = ranges[j];
                if (i != j && range1.intersect(range2) != null) {
                    throw new IllegalArgumentException("Key hash ranges with overlap between " + range1
                            + " and " + range2);
                }
            }
        }
        conf.setKeyHashRanges(Arrays.asList(ranges));
        return this;
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
