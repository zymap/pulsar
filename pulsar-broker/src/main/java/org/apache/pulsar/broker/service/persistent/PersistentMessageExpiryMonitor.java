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
package org.apache.pulsar.broker.service.persistent;

<<<<<<< HEAD
=======
import java.util.Optional;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
<<<<<<< HEAD
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.util.Rate;
import org.apache.pulsar.client.impl.MessageImpl;
=======
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.stats.Rate;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentMessageExpiryMonitor implements FindEntryCallback {
    private final ManagedCursor cursor;
    private final String subName;
    private final String topicName;
    private final Rate msgExpired;
<<<<<<< HEAD
=======
    private final boolean autoSkipNonRecoverableData;
    private final PersistentSubscription subscription;
>>>>>>> f773c602c... Test pr 10 (#27)

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    @SuppressWarnings("unused")
    private volatile int expirationCheckInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<PersistentMessageExpiryMonitor> expirationCheckInProgressUpdater = AtomicIntegerFieldUpdater
            .newUpdater(PersistentMessageExpiryMonitor.class, "expirationCheckInProgress");

<<<<<<< HEAD
    public PersistentMessageExpiryMonitor(String topicName, String subscriptionName, ManagedCursor cursor) {
        this.topicName = topicName;
        this.cursor = cursor;
        this.subName = subscriptionName;
        this.msgExpired = new Rate();
=======
    public PersistentMessageExpiryMonitor(String topicName, String subscriptionName, ManagedCursor cursor, PersistentSubscription subscription) {
        this.topicName = topicName;
        this.cursor = cursor;
        this.subName = subscriptionName;
        this.subscription = subscription;
        this.msgExpired = new Rate();
        // check to avoid test failures
        this.autoSkipNonRecoverableData = this.cursor.getManagedLedger() != null
                && this.cursor.getManagedLedger().getConfig().isAutoSkipNonRecoverableData();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public void expireMessages(int messageTTLInSeconds) {
        if (expirationCheckInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            log.info("[{}][{}] Starting message expiry check, ttl= {} seconds", topicName, subName,
                    messageTTLInSeconds);

            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchActiveEntries, entry -> {
<<<<<<< HEAD
                MessageImpl msg = null;
=======
                MessageImpl<?> msg = null;
>>>>>>> f773c602c... Test pr 10 (#27)
                try {
                    msg = MessageImpl.deserialize(entry.getDataBuffer());
                    return msg.isExpired(messageTTLInSeconds);
                } catch (Exception e) {
                    log.error("[{}][{}] Error deserializing message for expiry check", topicName, subName, e);
                } finally {
                    entry.release();
                    if (msg != null) {
                        msg.recycle();
                    }
                }
                return false;
            }, this, null);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore expire-message scheduled task, last check is still running", topicName,
                        subName);
            }
        }
    }

    public void updateRates() {
        msgExpired.calculateRate();
    }

    public double getMessageExpiryRate() {
        return msgExpired.getRate();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentMessageExpiryMonitor.class);

    private final MarkDeleteCallback markDeleteCallback = new MarkDeleteCallback() {
        @Override
        public void markDeleteComplete(Object ctx) {
<<<<<<< HEAD
            long numMessagesExpired = (long) ctx - cursor.getNumberOfEntriesInBacklog();
            msgExpired.recordMultipleEvents(numMessagesExpired, 0 /* no value stats */);
            updateRates();

=======
            long numMessagesExpired = (long) ctx - cursor.getNumberOfEntriesInBacklog(false);
            msgExpired.recordMultipleEvents(numMessagesExpired, 0 /* no value stats */);
            updateRates();
            // If the subscription is a Key_Shared subscription, we should to trigger message dispatch.
            if (subscription != null && subscription.getType() == PulsarApi.CommandSubscribe.SubType.Key_Shared) {
                subscription.getDispatcher().acknowledgementWasProcessed();
            }
>>>>>>> f773c602c... Test pr 10 (#27)
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Mark deleted {} messages", topicName, subName, numMessagesExpired);
            }
        }

        @Override
        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
            log.warn("[{}][{}] Message expiry failed - mark delete failed", topicName, subName, exception);
            updateRates();
        }
    };

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        if (position != null) {
            log.info("[{}][{}] Expiring all messages until position {}", topicName, subName, position);
<<<<<<< HEAD
            cursor.asyncMarkDelete(position, markDeleteCallback, cursor.getNumberOfEntriesInBacklog());
=======
            cursor.asyncMarkDelete(position, markDeleteCallback, cursor.getNumberOfEntriesInBacklog(false));
>>>>>>> f773c602c... Test pr 10 (#27)
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] No messages to expire", topicName, subName);
            }
            updateRates();
        }
        expirationCheckInProgress = FALSE;
    }

    @Override
<<<<<<< HEAD
    public void findEntryFailed(ManagedLedgerException exception, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Finding expired entry operation failed", topicName, subName, exception);
        }
=======
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Finding expired entry operation failed", topicName, subName, exception);
        }
        if (autoSkipNonRecoverableData && failedReadPosition.isPresent()
                && (exception instanceof NonRecoverableLedgerException)) {
            log.warn("[{}][{}] read failed from ledger at position:{} : {}", topicName, subName, failedReadPosition,
                    exception.getMessage());
            findEntryComplete(failedReadPosition.get(), ctx);
        }
>>>>>>> f773c602c... Test pr 10 (#27)
        expirationCheckInProgress = FALSE;
        updateRates();
    }
}
