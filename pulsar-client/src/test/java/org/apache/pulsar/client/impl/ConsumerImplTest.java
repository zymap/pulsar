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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
=======
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

<<<<<<< HEAD
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

public class ConsumerImplTest {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ConsumerImpl<ConsumerImpl> consumer;
=======
public class ConsumerImplTest {


    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ConsumerImpl<byte[]> consumer;
>>>>>>> f773c602c... Test pr 10 (#27)
    private ConsumerConfigurationData consumerConf;

    @BeforeMethod
    public void setUp() {
        consumerConf = new ConsumerConfigurationData<>();
<<<<<<< HEAD
        ClientConfigurationData clientConf = new ClientConfigurationData();
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        CompletableFuture<ClientCnx> clientCnxFuture = new CompletableFuture<>();
        CompletableFuture<Consumer<ConsumerImpl>> subscribeFuture = new CompletableFuture<>();
        String topic = "non-persistent://tenant/ns1/my-topic";

        // Mock connection for grabCnx()
        when(client.getConnection(anyString())).thenReturn(clientCnxFuture);
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        when(client.getConfiguration()).thenReturn(clientConf);

        consumerConf.setSubscriptionName("test-sub");
        consumer = new ConsumerImpl<ConsumerImpl>(client, topic, consumerConf,
                executorService, -1, subscribeFuture, null, null);
=======
        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock();
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        CompletableFuture<Consumer<ConsumerImpl>> subscribeFuture = new CompletableFuture<>();
        String topic = "non-persistent://tenant/ns1/my-topic";

        consumerConf.setSubscriptionName("test-sub");
        consumer = ConsumerImpl.newConsumerImpl(client, topic, consumerConf,
                executorService, -1, false, subscribeFuture, null, null, null,
                true);
        consumer.setState(HandlerState.State.Ready);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_EmptyQueueNotThrowsException() {
        consumer.notifyPendingReceivedCallback(null, null);
    }

<<<<<<< HEAD
    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithException() {
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
=======
    @Test(invocationTimeOut = 500)
    public void testCorrectBackoffConfiguration() {
        final Backoff backoff = consumer.getConnectionHandler().backoff;
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        Assert.assertEquals(backoff.getMax(), TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getMaxBackoffIntervalNanos()));
        Assert.assertEquals(backoff.next(), TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getInitialBackoffIntervalNanos()));
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithException() {
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
>>>>>>> f773c602c... Test pr 10 (#27)
        consumer.pendingReceives.add(receiveFuture);
        Exception exception = new PulsarClientException.InvalidMessageException("some random exception");
        consumer.notifyPendingReceivedCallback(null, exception);

        try {
            receiveFuture.join();
        } catch (CompletionException e) {
            // Completion exception must be the same we provided at calling time
            Assert.assertEquals(e.getCause(), exception);
        }

        Assert.assertTrue(receiveFuture.isCompletedExceptionally());
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithExceptionWhenMessageIsNull() {
<<<<<<< HEAD
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
=======
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
>>>>>>> f773c602c... Test pr 10 (#27)
        consumer.pendingReceives.add(receiveFuture);
        consumer.notifyPendingReceivedCallback(null, null);

        try {
            receiveFuture.join();
        } catch (CompletionException e) {
            Assert.assertEquals("received message can't be null", e.getCause().getMessage());
        }

        Assert.assertTrue(receiveFuture.isCompletedExceptionally());
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_InterceptorsWorksWithPrefetchDisabled() {
<<<<<<< HEAD
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<ConsumerImpl> spy = spy(consumer);
=======
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<byte[]> spy = spy(consumer);
>>>>>>> f773c602c... Test pr 10 (#27)

        consumer.pendingReceives.add(receiveFuture);
        consumerConf.setReceiverQueueSize(0);
        doReturn(message).when(spy).beforeConsume(any());
        spy.notifyPendingReceivedCallback(message, null);
<<<<<<< HEAD
        Message<ConsumerImpl> receivedMessage = receiveFuture.join();
=======
        Message<byte[]> receivedMessage = receiveFuture.join();
>>>>>>> f773c602c... Test pr 10 (#27)

        verify(spy, times(1)).beforeConsume(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_WorkNormally() {
<<<<<<< HEAD
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<ConsumerImpl> spy = spy(consumer);
=======
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<byte[]> spy = spy(consumer);
>>>>>>> f773c602c... Test pr 10 (#27)

        consumer.pendingReceives.add(receiveFuture);
        doReturn(message).when(spy).beforeConsume(any());
        doNothing().when(spy).messageProcessed(message);
        spy.notifyPendingReceivedCallback(message, null);
<<<<<<< HEAD
        Message<ConsumerImpl> receivedMessage = receiveFuture.join();
=======
        Message<byte[]> receivedMessage = receiveFuture.join();
>>>>>>> f773c602c... Test pr 10 (#27)

        verify(spy, times(1)).beforeConsume(message);
        verify(spy, times(1)).messageProcessed(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }
<<<<<<< HEAD
=======

    @Test
    public void testReceiveAsyncCanBeCancelled() {
        // given
        CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
        Assert.assertEquals(consumer.peekPendingReceive(), future);
        // when
        future.cancel(true);
        // then
        Assert.assertTrue(consumer.pendingReceives.isEmpty());
    }

    @Test
    public void testBatchReceiveAsyncCanBeCancelled() {
        // given
        CompletableFuture<Messages<byte[]>> future = consumer.batchReceiveAsync();
        Assert.assertTrue(consumer.hasPendingBatchReceive());
        // when
        future.cancel(true);
        // then
        Assert.assertFalse(consumer.hasPendingBatchReceive());
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
