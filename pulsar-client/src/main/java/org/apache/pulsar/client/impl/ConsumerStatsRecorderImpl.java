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

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

public class ConsumerStatsRecorderImpl implements ConsumerStatsRecorder {

    private static final long serialVersionUID = 1L;
    private TimerTask stat;
    private Timeout statTimeout;
    private ConsumerImpl<?> consumer;
    private PulsarClientImpl pulsarClient;
    private long oldTime;
    private long statsIntervalSeconds;
    private final LongAdder numMsgsReceived;
    private final LongAdder numBytesReceived;
    private final LongAdder numReceiveFailed;
<<<<<<< HEAD
=======
    private final LongAdder numBatchReceiveFailed;
>>>>>>> f773c602c... Test pr 10 (#27)
    private final LongAdder numAcksSent;
    private final LongAdder numAcksFailed;
    private final LongAdder totalMsgsReceived;
    private final LongAdder totalBytesReceived;
    private final LongAdder totalReceiveFailed;
<<<<<<< HEAD
=======
    private final LongAdder totalBatchReceiveFailed;
>>>>>>> f773c602c... Test pr 10 (#27)
    private final LongAdder totalAcksSent;
    private final LongAdder totalAcksFailed;

    private volatile double receivedMsgsRate;
    private volatile double receivedBytesRate;

    private static final DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat("0.00");

    public ConsumerStatsRecorderImpl() {
<<<<<<< HEAD
        numMsgsReceived = null;
        numBytesReceived = null;
        numReceiveFailed = null;
        numAcksSent = null;
        numAcksFailed = null;
        totalMsgsReceived = null;
        totalBytesReceived = null;
        totalReceiveFailed = null;
        totalAcksSent = null;
        totalAcksFailed = null;
=======
        numMsgsReceived = new LongAdder();
        numBytesReceived = new LongAdder();
        numReceiveFailed = new LongAdder();
        numBatchReceiveFailed = new LongAdder();
        numAcksSent = new LongAdder();
        numAcksFailed = new LongAdder();
        totalMsgsReceived = new LongAdder();
        totalBytesReceived = new LongAdder();
        totalReceiveFailed = new LongAdder();
        totalBatchReceiveFailed = new LongAdder();
        totalAcksSent = new LongAdder();
        totalAcksFailed = new LongAdder();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public ConsumerStatsRecorderImpl(PulsarClientImpl pulsarClient, ConsumerConfigurationData<?> conf,
            ConsumerImpl<?> consumer) {
        this.pulsarClient = pulsarClient;
        this.consumer = consumer;
        this.statsIntervalSeconds = pulsarClient.getConfiguration().getStatsIntervalSeconds();
        numMsgsReceived = new LongAdder();
        numBytesReceived = new LongAdder();
        numReceiveFailed = new LongAdder();
<<<<<<< HEAD
=======
        numBatchReceiveFailed = new LongAdder();
>>>>>>> f773c602c... Test pr 10 (#27)
        numAcksSent = new LongAdder();
        numAcksFailed = new LongAdder();
        totalMsgsReceived = new LongAdder();
        totalBytesReceived = new LongAdder();
        totalReceiveFailed = new LongAdder();
<<<<<<< HEAD
=======
        totalBatchReceiveFailed = new LongAdder();
>>>>>>> f773c602c... Test pr 10 (#27)
        totalAcksSent = new LongAdder();
        totalAcksFailed = new LongAdder();
        init(conf);
    }

    private void init(ConsumerConfigurationData<?> conf) {
        ObjectMapper m = new ObjectMapper();
        m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();

        try {
<<<<<<< HEAD
            log.info("Starting Pulsar consumer perf with config: {}", w.writeValueAsString(conf));
            log.info("Pulsar client config: {}", w.withoutAttribute("authentication").writeValueAsString(pulsarClient.getConfiguration()));
        } catch (IOException e) {
            log.error("Failed to dump config info: {}", e);
=======
            log.info("Starting Pulsar consumer status recorder with config: {}", w.writeValueAsString(conf));
            log.info("Pulsar client config: {}", w.withoutAttribute("authentication").writeValueAsString(pulsarClient.getConfiguration()));
        } catch (IOException e) {
            log.error("Failed to dump config info", e);
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        stat = (timeout) -> {
            if (timeout.isCancelled()) {
                return;
            }
            try {
                long now = System.nanoTime();
                double elapsed = (now - oldTime) / 1e9;
                oldTime = now;
                long currentNumMsgsReceived = numMsgsReceived.sumThenReset();
                long currentNumBytesReceived = numBytesReceived.sumThenReset();
                long currentNumReceiveFailed = numReceiveFailed.sumThenReset();
<<<<<<< HEAD
=======
                long currentNumBatchReceiveFailed = numBatchReceiveFailed.sumThenReset();
>>>>>>> f773c602c... Test pr 10 (#27)
                long currentNumAcksSent = numAcksSent.sumThenReset();
                long currentNumAcksFailed = numAcksFailed.sumThenReset();

                totalMsgsReceived.add(currentNumMsgsReceived);
                totalBytesReceived.add(currentNumBytesReceived);
                totalReceiveFailed.add(currentNumReceiveFailed);
<<<<<<< HEAD
=======
                totalBatchReceiveFailed.add(currentNumBatchReceiveFailed);
>>>>>>> f773c602c... Test pr 10 (#27)
                totalAcksSent.add(currentNumAcksSent);
                totalAcksFailed.add(currentNumAcksFailed);

                receivedMsgsRate = currentNumMsgsReceived / elapsed;
                receivedBytesRate = currentNumBytesReceived / elapsed;

                if ((currentNumMsgsReceived | currentNumBytesReceived | currentNumReceiveFailed | currentNumAcksSent
                        | currentNumAcksFailed) != 0) {
                    log.info(
                            "[{}] [{}] [{}] Prefetched messages: {} --- "
                                    + "Consume throughput received: {} msgs/s --- {} Mbit/s --- "
<<<<<<< HEAD
                                    + "Ack sent rate: {} ack/s --- " + "Failed messages: {} --- " + "Failed acks: {}",
=======
                                    + "Ack sent rate: {} ack/s --- " + "Failed messages: {} --- batch messages: {} ---"
                                    + "Failed acks: {}",
>>>>>>> f773c602c... Test pr 10 (#27)
                            consumer.getTopic(), consumer.getSubscription(), consumer.consumerName,
                            consumer.incomingMessages.size(), THROUGHPUT_FORMAT.format(receivedMsgsRate),
                            THROUGHPUT_FORMAT.format(receivedBytesRate * 8 / 1024 / 1024),
                            THROUGHPUT_FORMAT.format(currentNumAcksSent / elapsed), currentNumReceiveFailed,
<<<<<<< HEAD
                            currentNumAcksFailed);
=======
                            currentNumBatchReceiveFailed, currentNumAcksFailed);
>>>>>>> f773c602c... Test pr 10 (#27)
                }
            } catch (Exception e) {
                log.error("[{}] [{}] [{}]: {}", consumer.getTopic(), consumer.subscription, consumer.consumerName,
                        e.getMessage());
            } finally {
                // schedule the next stat info
                statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
            }
        };

        oldTime = System.nanoTime();
        statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void updateNumMsgsReceived(Message<?> message) {
        if (message != null) {
            numMsgsReceived.increment();
<<<<<<< HEAD
            numBytesReceived.add(message.getData().length);
=======
            numBytesReceived.add(message.getData() == null ? 0 : message.getData().length);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    @Override
    public void incrementNumAcksSent(long numAcks) {
        numAcksSent.add(numAcks);
    }

    @Override
    public void incrementNumAcksFailed() {
        numAcksFailed.increment();
    }

    @Override
    public void incrementNumReceiveFailed() {
        numReceiveFailed.increment();
    }

    @Override
<<<<<<< HEAD
=======
    public void incrementNumBatchReceiveFailed() {
        numBatchReceiveFailed.increment();
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public Optional<Timeout> getStatTimeout() {
        return Optional.ofNullable(statTimeout);
    }

    @Override
    public void reset() {
        numMsgsReceived.reset();
        numBytesReceived.reset();
        numReceiveFailed.reset();
<<<<<<< HEAD
=======
        numBatchReceiveFailed.reset();
>>>>>>> f773c602c... Test pr 10 (#27)
        numAcksSent.reset();
        numAcksFailed.reset();
        totalMsgsReceived.reset();
        totalBytesReceived.reset();
        totalReceiveFailed.reset();
<<<<<<< HEAD
=======
        totalBatchReceiveFailed.reset();
>>>>>>> f773c602c... Test pr 10 (#27)
        totalAcksSent.reset();
        totalAcksFailed.reset();
    }

    @Override
    public void updateCumulativeStats(ConsumerStats stats) {
        if (stats == null) {
            return;
        }
        numMsgsReceived.add(stats.getNumMsgsReceived());
        numBytesReceived.add(stats.getNumBytesReceived());
        numReceiveFailed.add(stats.getNumReceiveFailed());
<<<<<<< HEAD
=======
        numBatchReceiveFailed.add(stats.getNumBatchReceiveFailed());
>>>>>>> f773c602c... Test pr 10 (#27)
        numAcksSent.add(stats.getNumAcksSent());
        numAcksFailed.add(stats.getNumAcksFailed());
        totalMsgsReceived.add(stats.getTotalMsgsReceived());
        totalBytesReceived.add(stats.getTotalBytesReceived());
        totalReceiveFailed.add(stats.getTotalReceivedFailed());
<<<<<<< HEAD
=======
        totalBatchReceiveFailed.add(stats.getTotaBatchReceivedFailed());
>>>>>>> f773c602c... Test pr 10 (#27)
        totalAcksSent.add(stats.getTotalAcksSent());
        totalAcksFailed.add(stats.getTotalAcksFailed());
    }

    public long getNumMsgsReceived() {
        return numMsgsReceived.longValue();
    }

    public long getNumBytesReceived() {
        return numBytesReceived.longValue();
    }

    public long getNumAcksSent() {
        return numAcksSent.longValue();
    }

    public long getNumAcksFailed() {
        return numAcksFailed.longValue();
    }

    public long getNumReceiveFailed() {
        return numReceiveFailed.longValue();
    }

<<<<<<< HEAD
=======
    @Override
    public long getNumBatchReceiveFailed() {
        return numBatchReceiveFailed.longValue();
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    public long getTotalMsgsReceived() {
        return totalMsgsReceived.longValue();
    }

    public long getTotalBytesReceived() {
        return totalBytesReceived.longValue();
    }

    public long getTotalReceivedFailed() {
        return totalReceiveFailed.longValue();
    }

<<<<<<< HEAD
=======
    @Override
    public long getTotaBatchReceivedFailed() {
        return totalBatchReceiveFailed.longValue();
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    public long getTotalAcksSent() {
        return totalAcksSent.longValue();
    }

    public long getTotalAcksFailed() {
        return totalAcksFailed.longValue();
    }

    @Override
    public double getRateMsgsReceived() {
        return receivedMsgsRate;
    }

    @Override
    public double getRateBytesReceived() {
        return receivedBytesRate;
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerStatsRecorderImpl.class);
<<<<<<< HEAD
}
=======
}
>>>>>>> f773c602c... Test pr 10 (#27)
