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
package org.apache.pulsar.broker.service.nonpersistent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
<<<<<<< HEAD
import org.apache.bookkeeper.mledger.util.Rate;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
<<<<<<< HEAD
import org.apache.pulsar.utils.CopyOnWriteArrayList;


public interface NonPersistentDispatcher extends Dispatcher{
=======
import org.apache.pulsar.common.stats.Rate;


public interface NonPersistentDispatcher extends Dispatcher {
>>>>>>> f773c602c... Test pr 10 (#27)

    void addConsumer(Consumer consumer) throws BrokerServiceException;

    void removeConsumer(Consumer consumer) throws BrokerServiceException ;

    boolean isConsumerConnected();
<<<<<<< HEAD
    
    CopyOnWriteArrayList<Consumer> getConsumers();
=======

    List<Consumer> getConsumers();
>>>>>>> f773c602c... Test pr 10 (#27)

    boolean canUnsubscribe(Consumer consumer);

    CompletableFuture<Void> close() ;

<<<<<<< HEAD
    CompletableFuture<Void> disconnectAllConsumers();
=======
    CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor);
>>>>>>> f773c602c... Test pr 10 (#27)

    void reset();

    SubType getType();
<<<<<<< HEAD
    
    void sendMessages(List<Entry> entries);
    
    Rate getMesssageDropRate();
    
    boolean hasPermits();
    
=======

    void sendMessages(List<Entry> entries);

    Rate getMessageDropRate();

    boolean hasPermits();

>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    default void redeliverUnacknowledgedMessages(Consumer consumer) {
        // No-op
    }

    @Override
    default void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        // No-op
    }

    @Override
    default void addUnAckedMessages(int unAckMessages) {
        // No-op
    }

}
