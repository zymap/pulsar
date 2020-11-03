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
package org.apache.pulsar.common.policies.data;

<<<<<<< HEAD
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
public class ConsumerStats {
    /** Total rate of messages delivered to the consumer. msg/s */
    public double msgRateOut;

    /** Total throughput delivered to the consumer. bytes/s */
    public double msgThroughputOut;

    /** Total rate of messages redelivered by this consumer. msg/s */
    public double msgRateRedeliver;

    /** Name of the consumer */
    public String consumerName;

    /** Number of available message permits for the consumer */
    public int availablePermits;

    /** Number of unacknowledged messages for the consumer */
    public int unackedMessages;

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages */
    public boolean blockedConsumerOnUnackedMsgs;

    /** Address of this consumer */
    private int addressOffset = -1;
    private int addressLength;

    /** Timestamp of connection */
    private int connectedSinceOffset = -1;
    private int connectedSinceLength;

    /** Client library version */
    private int clientVersionOffset = -1;
    private int clientVersionLength;

    /** Metadata (key/value strings) associated with this consumer */
    public Map<String, String> metadata;

    /**
     * In order to prevent multiple string object allocation under stats: create a string-buffer that stores data for all string
     * place-holders
=======
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

/**
 * Consumer statistics.
 */
public class ConsumerStats {
    /** Total rate of messages delivered to the consumer (msg/s). */
    public double msgRateOut;

    /** Total throughput delivered to the consumer (bytes/s). */
    public double msgThroughputOut;

    /** Total bytes delivered to consumer (bytes). */
    public long bytesOutCounter;

    /** Total messages delivered to consumer (msg). */
    public long msgOutCounter;

    /** Total rate of messages redelivered by this consumer (msg/s). */
    public double msgRateRedeliver;

    /** Total chunked messages dispatched. */
    public double chuckedMessageRate;

    /** Name of the consumer. */
    public String consumerName;

    /** Number of available message permits for the consumer. */
    public int availablePermits;

    /** Number of unacknowledged messages for the consumer. */
    public int unackedMessages;

    /** Number of average messages per entry for the consumer consumed. */
    public int avgMessagesPerEntry;

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages. */
    public boolean blockedConsumerOnUnackedMsgs;

    /** The read position of the cursor when the consumer joining. */
    public String readPositionWhenJoining;

    /** Address of this consumer. */
    private int addressOffset = -1;
    private int addressLength;

    /** Timestamp of connection. */
    private int connectedSinceOffset = -1;
    private int connectedSinceLength;

    /** Client library version. */
    private int clientVersionOffset = -1;
    private int clientVersionLength;

    public long lastAckedTimestamp;
    public long lastConsumedTimestamp;

    /** Metadata (key/value strings) associated with this consumer. */
    public Map<String, String> metadata;

    /**
     * In order to prevent multiple string object allocation under stats: create a string-buffer
     * that stores data for all string place-holders.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    private StringBuilder stringBuffer = new StringBuilder();

    public ConsumerStats add(ConsumerStats stats) {
        checkNotNull(stats);
        this.msgRateOut += stats.msgRateOut;
        this.msgThroughputOut += stats.msgThroughputOut;
<<<<<<< HEAD
=======
        this.bytesOutCounter += stats.bytesOutCounter;
        this.msgOutCounter += stats.msgOutCounter;
>>>>>>> f773c602c... Test pr 10 (#27)
        this.msgRateRedeliver += stats.msgRateRedeliver;
        this.availablePermits += stats.availablePermits;
        this.unackedMessages += stats.unackedMessages;
        this.blockedConsumerOnUnackedMsgs = stats.blockedConsumerOnUnackedMsgs;
<<<<<<< HEAD
=======
        this.readPositionWhenJoining = stats.readPositionWhenJoining;
>>>>>>> f773c602c... Test pr 10 (#27)
        return this;
    }

    public String getAddress() {
        return addressOffset == -1 ? null : stringBuffer.substring(addressOffset, addressOffset + addressLength);
    }

    public void setAddress(String address) {
        if (address == null) {
            this.addressOffset = -1;
            return;
        }
        this.addressOffset = this.stringBuffer.length();
        this.addressLength = address.length();
        this.stringBuffer.append(address);
    }

    public String getConnectedSince() {
        return connectedSinceOffset == -1 ? null
                : stringBuffer.substring(connectedSinceOffset, connectedSinceOffset + connectedSinceLength);
    }

    public void setConnectedSince(String connectedSince) {
        if (connectedSince == null) {
            this.connectedSinceOffset = -1;
            return;
        }
        this.connectedSinceOffset = this.stringBuffer.length();
        this.connectedSinceLength = connectedSince.length();
        this.stringBuffer.append(connectedSince);
    }

    public String getClientVersion() {
        return clientVersionOffset == -1 ? null
                : stringBuffer.substring(clientVersionOffset, clientVersionOffset + clientVersionLength);
    }

    public void setClientVersion(String clientVersion) {
        if (clientVersion == null) {
            this.clientVersionOffset = -1;
            return;
        }
        this.clientVersionOffset = this.stringBuffer.length();
        this.clientVersionLength = clientVersion.length();
        this.stringBuffer.append(clientVersion);
    }
<<<<<<< HEAD
=======

    public String getReadPositionWhenJoining() {
        return readPositionWhenJoining;
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
