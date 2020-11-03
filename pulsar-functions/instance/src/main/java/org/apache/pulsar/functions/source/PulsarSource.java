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
package org.apache.pulsar.functions.source;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;

import java.util.*;
<<<<<<< HEAD
import java.util.concurrent.CompletableFuture;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
<<<<<<< HEAD
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Utils;
=======
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.util.Reflections;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

@Slf4j
public class PulsarSource<T> extends PushSource<T> implements MessageListener<T> {

    private final PulsarClient pulsarClient;
    private final PulsarSourceConfig pulsarSourceConfig;
    private final Map<String, String> properties;
<<<<<<< HEAD
    private List<String> inputTopics;
    private List<Consumer<T>> inputConsumers;
    private final TopicSchema topicSchema;

    public PulsarSource(PulsarClient pulsarClient, PulsarSourceConfig pulsarConfig, Map<String, String> properties) {
=======
    private final ClassLoader functionClassLoader;
    private List<String> inputTopics;
    private List<Consumer<T>> inputConsumers = new LinkedList<>();
    private final TopicSchema topicSchema;

    public PulsarSource(PulsarClient pulsarClient, PulsarSourceConfig pulsarConfig, Map<String, String> properties,
                        ClassLoader functionClassLoader) {
>>>>>>> f773c602c... Test pr 10 (#27)
        this.pulsarClient = pulsarClient;
        this.pulsarSourceConfig = pulsarConfig;
        this.topicSchema = new TopicSchema(pulsarClient);
        this.properties = properties;
<<<<<<< HEAD
=======
        this.functionClassLoader = functionClassLoader;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        // Setup schemas
        log.info("Opening pulsar source with config: {}", pulsarSourceConfig);
        Map<String, ConsumerConfig<T>> configs = setupConsumerConfigs();

<<<<<<< HEAD
        inputConsumers = configs.entrySet().stream().map(e -> {
            String topic = e.getKey();
            ConsumerConfig<T> conf = e.getValue();
            log.info("Creating consumers for topic : {}, schema : {}",  topic, conf.getSchema());
=======
        for (Map.Entry<String, ConsumerConfig<T>> e : configs.entrySet()) {
            String topic = e.getKey();
            ConsumerConfig<T> conf = e.getValue();
            log.info("Creating consumers for topic : {}, schema : {}, schemaInfo: {}",
                    topic, conf.getSchema(), conf.getSchema().getSchemaInfo());

>>>>>>> f773c602c... Test pr 10 (#27)
            ConsumerBuilder<T> cb = pulsarClient.newConsumer(conf.getSchema())
                    // consume message even if can't decrypt and deliver it along with encryption-ctx
                    .cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                    .subscriptionName(pulsarSourceConfig.getSubscriptionName())
<<<<<<< HEAD
                    .subscriptionType(pulsarSourceConfig.getSubscriptionType())
                    .messageListener(this);

            if (conf.isRegexPattern) {
                cb.topicsPattern(topic);
            } else {
                cb.topic(topic);
            }
            cb.properties(properties);

            if (pulsarSourceConfig.getTimeoutMs() != null) {
                cb.ackTimeout(pulsarSourceConfig.getTimeoutMs(), TimeUnit.MILLISECONDS);
            }

=======
                    .subscriptionInitialPosition(pulsarSourceConfig.getSubscriptionPosition())
                    .subscriptionType(pulsarSourceConfig.getSubscriptionType());

            if (conf.getConsumerProperties() != null && !conf.getConsumerProperties().isEmpty()) {
                cb.loadConf(new HashMap<>(conf.getConsumerProperties()));
            }
            //messageListener is annotated with @JsonIgnore,so setting messageListener should be put behind loadConf
            cb.messageListener(this);

            if (conf.isRegexPattern) {
                cb = cb.topicsPattern(topic);
            } else {
                cb = cb.topics(Collections.singletonList(topic));
            }
            if (conf.getReceiverQueueSize() != null) {
                cb = cb.receiverQueueSize(conf.getReceiverQueueSize());
            }
            cb = cb.properties(properties);
            if (pulsarSourceConfig.getNegativeAckRedeliveryDelayMs() != null
                    && pulsarSourceConfig.getNegativeAckRedeliveryDelayMs() > 0) {
                cb.negativeAckRedeliveryDelay(pulsarSourceConfig.getNegativeAckRedeliveryDelayMs(), TimeUnit.MILLISECONDS);
            }
            if (pulsarSourceConfig.getTimeoutMs() != null) {
                cb = cb.ackTimeout(pulsarSourceConfig.getTimeoutMs(), TimeUnit.MILLISECONDS);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
            if (pulsarSourceConfig.getMaxMessageRetries() != null && pulsarSourceConfig.getMaxMessageRetries() >= 0) {
                DeadLetterPolicy.DeadLetterPolicyBuilder deadLetterPolicyBuilder = DeadLetterPolicy.builder();
                deadLetterPolicyBuilder.maxRedeliverCount(pulsarSourceConfig.getMaxMessageRetries());
                if (pulsarSourceConfig.getDeadLetterTopic() != null && !pulsarSourceConfig.getDeadLetterTopic().isEmpty()) {
                    deadLetterPolicyBuilder.deadLetterTopic(pulsarSourceConfig.getDeadLetterTopic());
                }
<<<<<<< HEAD
                cb.deadLetterPolicy(deadLetterPolicyBuilder.build());
            }

            return cb.subscribeAsync();
        }).collect(Collectors.toList()).stream().map(CompletableFuture::join).collect(Collectors.toList());
=======
                cb = cb.deadLetterPolicy(deadLetterPolicyBuilder.build());
            }

            Consumer<T> consumer = cb.subscribeAsync().join();
            inputConsumers.add(consumer);
        }
>>>>>>> f773c602c... Test pr 10 (#27)

        inputTopics = inputConsumers.stream().flatMap(c -> {
            return (c instanceof MultiTopicsConsumerImpl) ? ((MultiTopicsConsumerImpl<?>) c).getTopics().stream()
                    : Collections.singletonList(c.getTopic()).stream();
        }).collect(Collectors.toList());
    }

    @Override
    public void received(Consumer<T> consumer, Message<T> message) {

        Record<T> record = PulsarRecord.<T>builder()
                .message(message)
                .topicName(message.getTopicName())
                .ackFunction(() -> {
                    if (pulsarSourceConfig
                            .getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                        consumer.acknowledgeCumulativeAsync(message);
                    } else {
                        consumer.acknowledgeAsync(message);
                    }
                }).failFunction(() -> {
                    if (pulsarSourceConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                        throw new RuntimeException("Failed to process message: " + message.getMessageId());
                    }
<<<<<<< HEAD
=======
                    consumer.negativeAcknowledge(message);
>>>>>>> f773c602c... Test pr 10 (#27)
                })
                .build();

        consume(record);
    }

    @Override
    public void close() throws Exception {
        if (inputConsumers != null ) {
            inputConsumers.forEach(consumer -> {
                try {
                    consumer.close();
                } catch (PulsarClientException e) {
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    Map<String, ConsumerConfig<T>> setupConsumerConfigs() throws ClassNotFoundException {
        Map<String, ConsumerConfig<T>> configs = new TreeMap<>();

        Class<?> typeArg = Reflections.loadClass(this.pulsarSourceConfig.getTypeClassName(),
<<<<<<< HEAD
                Thread.currentThread().getContextClassLoader());
=======
                this.functionClassLoader);
>>>>>>> f773c602c... Test pr 10 (#27)

        checkArgument(!Void.class.equals(typeArg), "Input type of Pulsar Function cannot be Void");

        // Check new config with schema types or classnames
        pulsarSourceConfig.getTopicSchema().forEach((topic, conf) -> {
            Schema<T> schema;
            if (conf.getSerdeClassName() != null && !conf.getSerdeClassName().isEmpty()) {
                schema = (Schema<T>) topicSchema.getSchema(topic, typeArg, conf.getSerdeClassName(), true);
            } else {
<<<<<<< HEAD
                schema = (Schema<T>) topicSchema.getSchema(topic, typeArg, conf.getSchemaType(), true);
            }
            configs.put(topic,
                    ConsumerConfig.<T> builder().schema(schema).isRegexPattern(conf.isRegexPattern()).build());
=======
                schema = (Schema<T>) topicSchema.getSchema(topic, typeArg, conf, true);
            }
            configs.put(topic,
                    ConsumerConfig.<T> builder().
                            schema(schema).
                            isRegexPattern(conf.isRegexPattern()).
                            receiverQueueSize(conf.getReceiverQueueSize()).
                            consumerProperties(conf.getConsumerProperties()).build());
>>>>>>> f773c602c... Test pr 10 (#27)
        });

        return configs;
    }

    public List<String> getInputTopics() {
        return inputTopics;
    }

<<<<<<< HEAD
=======
    public List<Consumer<T>> getInputConsumers() {
        return inputConsumers;
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    @Data
    @Builder
    private static class ConsumerConfig<T> {
        private Schema<T> schema;
        private boolean isRegexPattern;
<<<<<<< HEAD
=======
        private Integer receiverQueueSize;
        private Map<String, String> consumerProperties;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

}
