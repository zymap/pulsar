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
package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
<<<<<<< HEAD

=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.Messages;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
<<<<<<< HEAD
import org.apache.pulsar.shade.io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
=======
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
>>>>>>> f773c602c... Test pr 10 (#27)

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

<<<<<<< HEAD
import static org.mockito.Matchers.any;
=======
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Tests for the PulsarConsumerSource. The source supports two operation modes.
 * 1) At-least-once (when checkpointed) with Pulsar message acknowledgements and the deduplication mechanism in
 *    {@link org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase}..
 * 3) No strong delivery guarantees (without checkpointing) with Pulsar acknowledging messages after
 *	  after it receives x number of messages.
 *
 * <p>This tests assumes that the MessageIds are increasing monotonously. That doesn't have to be the
 * case. The MessageId is used to uniquely identify messages.
 */
public class PulsarConsumerSourceTests {

    private PulsarConsumerSource<String> source;

    private TestConsumer consumer;

    private TestSourceContext context;

    private Thread sourceThread;

    private Exception exception;

<<<<<<< HEAD
    @Before
=======
    @BeforeMethod
>>>>>>> f773c602c... Test pr 10 (#27)
    public void before() {
        context = new TestSourceContext();

        sourceThread = new Thread(() -> {
            try {
                source.run(context);
            } catch (Exception e) {
                exception = e;
            }
        });
    }

<<<<<<< HEAD
    @After
=======
    @AfterMethod
>>>>>>> f773c602c... Test pr 10 (#27)
    public void after() throws Exception {
        if (source != null) {
            source.cancel();
        }
        if (sourceThread != null) {
            sourceThread.join();
        }
    }

    @Test
    public void testCheckpointing() throws Exception {
        final int numMessages = 5;
        consumer = new TestConsumer(numMessages);

        source = createSource(consumer, 1, true);
        source.open(new Configuration());

        final StreamSource<String, PulsarConsumerSource<String>> src = new StreamSource<>(source);
        final AbstractStreamOperatorTestHarness<String> testHarness =
            new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);

        testHarness.open();

        sourceThread.start();

        final Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 3; ++i) {

            // wait and receive messages from the test consumer
            receiveMessages();

            final long snapshotId = random.nextLong();
            OperatorSubtaskState data;
            synchronized (context.getCheckpointLock()) {
                data = testHarness.snapshot(snapshotId, System.currentTimeMillis());
            }

            final TestPulsarConsumerSource sourceCopy =
<<<<<<< HEAD
                createSource(Mockito.mock(Consumer.class), 1, true);
=======
                createSource(mock(Consumer.class), 1, true);
>>>>>>> f773c602c... Test pr 10 (#27)
            final StreamSource<String, TestPulsarConsumerSource> srcCopy = new StreamSource<>(sourceCopy);
            final AbstractStreamOperatorTestHarness<String> testHarnessCopy =
                new AbstractStreamOperatorTestHarness<>(srcCopy, 1, 1, 0);

            testHarnessCopy.setup();
            testHarnessCopy.initializeState(data);
            testHarnessCopy.open();

            final ArrayDeque<Tuple2<Long, Set<MessageId>>> deque = sourceCopy.getRestoredState();
            final Set<MessageId> messageIds = deque.getLast().f1;

            final int start = consumer.currentMessage.get() - numMessages;
            for (int mi = start; mi < (start + numMessages); ++mi) {
<<<<<<< HEAD
                Assert.assertTrue(messageIds.contains(consumer.messages.get(mi).getMessageId()));
=======
                assertTrue(messageIds.contains(consumer.messages.get(mi).getMessageId()));
>>>>>>> f773c602c... Test pr 10 (#27)
            }

            // check if the messages are being acknowledged
            synchronized (context.getCheckpointLock()) {
                source.notifyCheckpointComplete(snapshotId);

<<<<<<< HEAD
                Assert.assertEquals(consumer.acknowledgedIds.keySet(), messageIds);
=======
                assertEquals(consumer.acknowledgedIds.keySet(), messageIds);
>>>>>>> f773c602c... Test pr 10 (#27)
                // clear acknowledgements for the next snapshot comparison
                consumer.acknowledgedIds.clear();
            }

            final int lastMessageIndex = consumer.currentMessage.get();
            consumer.addMessages(createMessages(lastMessageIndex, 5));
        }
    }

    @Test
    public void testCheckpointingDuplicatedIds() throws Exception {
        consumer = new TestConsumer(5);

        source = createSource(consumer, 1, true);
        source.open(new Configuration());

        sourceThread.start();

        receiveMessages();

<<<<<<< HEAD
        Assert.assertEquals(5, context.elements.size());
=======
        assertEquals(5, context.elements.size());
>>>>>>> f773c602c... Test pr 10 (#27)

        // try to reprocess the messages we should not collect any more elements
        consumer.reset();

        receiveMessages();

<<<<<<< HEAD
        Assert.assertEquals(5, context.elements.size());
=======
        assertEquals(5, context.elements.size());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testCheckpointingDisabledMessagesEqualBatchSize() throws Exception {

        consumer = new TestConsumer(5);

        source = createSource(consumer, 5, false);
        source.open(new Configuration());

        sourceThread.start();

        receiveMessages();

<<<<<<< HEAD
        Assert.assertEquals(1, consumer.acknowledgedIds.size());
=======
        assertEquals(1, consumer.acknowledgedIds.size());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testCheckpointingDisabledMoreMessagesThanBatchSize() throws Exception {

        consumer = new TestConsumer(6);

        source = createSource(consumer, 5, false);
        source.open(new Configuration());

        sourceThread.start();

        receiveMessages();

<<<<<<< HEAD
        Assert.assertEquals(1, consumer.acknowledgedIds.size());
=======
        assertEquals(1, consumer.acknowledgedIds.size());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testCheckpointingDisabledLessMessagesThanBatchSize() throws Exception {

        consumer = new TestConsumer(4);

        source = createSource(consumer, 5, false);
        source.open(new Configuration());

        sourceThread.start();

        receiveMessages();

<<<<<<< HEAD
        Assert.assertEquals(0, consumer.acknowledgedIds.size());
=======
        assertEquals(0, consumer.acknowledgedIds.size());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testCheckpointingDisabledMessages2XBatchSize() throws Exception {

        consumer = new TestConsumer(10);

        source = createSource(consumer, 5, false);
        source.open(new Configuration());

        sourceThread.start();

        receiveMessages();

<<<<<<< HEAD
        Assert.assertEquals(2, consumer.acknowledgedIds.size());
=======
        assertEquals(2, consumer.acknowledgedIds.size());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void receiveMessages() throws InterruptedException {
        while (consumer.currentMessage.get() < consumer.messages.size()) {
            Thread.sleep(5);
        }
    }

    private TestPulsarConsumerSource createSource(Consumer<byte[]> testConsumer,
                                                  long batchSize, boolean isCheckpointingEnabled) throws Exception {
        PulsarSourceBuilder<String> builder =
            PulsarSourceBuilder.builder(new SimpleStringSchema())
                .acknowledgementBatchSize(batchSize);
        TestPulsarConsumerSource source = new TestPulsarConsumerSource(builder, testConsumer, isCheckpointingEnabled);

<<<<<<< HEAD
        OperatorStateStore mockStore = Mockito.mock(OperatorStateStore.class);
        FunctionInitializationContext mockContext = Mockito.mock(FunctionInitializationContext.class);
        Mockito.when(mockContext.getOperatorStateStore()).thenReturn(mockStore);
        Mockito.when(mockStore.getSerializableListState(any(String.class))).thenReturn(null);
=======
        OperatorStateStore mockStore = mock(OperatorStateStore.class);
        FunctionInitializationContext mockContext = mock(FunctionInitializationContext.class);
        when(mockContext.getOperatorStateStore()).thenReturn(mockStore);
        when(mockStore.getSerializableListState(any(String.class))).thenReturn(null);
>>>>>>> f773c602c... Test pr 10 (#27)

        source.initializeState(mockContext);

        return source;
    }

    private static class TestPulsarConsumerSource extends PulsarConsumerSource<String> {

        private ArrayDeque<Tuple2<Long, Set<MessageId>>> restoredState;

        private Consumer<byte[]> testConsumer;
        private boolean isCheckpointingEnabled;

        TestPulsarConsumerSource(PulsarSourceBuilder<String> builder,
                                 Consumer<byte[]> testConsumer, boolean isCheckpointingEnabled) {
            super(builder);
            this.testConsumer = testConsumer;
            this.isCheckpointingEnabled = isCheckpointingEnabled;
        }

        @Override
        protected boolean addId(MessageId messageId) {
<<<<<<< HEAD
            Assert.assertEquals(true, isCheckpointingEnabled());
=======
            assertTrue(isCheckpointingEnabled());
>>>>>>> f773c602c... Test pr 10 (#27)
            return super.addId(messageId);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
<<<<<<< HEAD
            StreamingRuntimeContext context = Mockito.mock(StreamingRuntimeContext.class);
            Mockito.when(context.isCheckpointingEnabled()).thenReturn(isCheckpointingEnabled);
=======
            StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
            when(context.isCheckpointingEnabled()).thenReturn(isCheckpointingEnabled);
>>>>>>> f773c602c... Test pr 10 (#27)
            return context;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            super.initializeState(context);
            this.restoredState = this.pendingCheckpoints;
        }

        public ArrayDeque<Tuple2<Long, Set<MessageId>>> getRestoredState() {
            return this.restoredState;
        }

        @Override
<<<<<<< HEAD
        PulsarClient createClient() {
            return Mockito.mock(PulsarClient.class);
=======
        PulsarClient getClient() {
            return mock(PulsarClient.class);
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        @Override
        Consumer<byte[]> createConsumer(PulsarClient client) {
            return testConsumer;
        }
    }

    private static class TestSourceContext implements SourceFunction.SourceContext<String> {

        private static final Object lock = new Object();

        private final List<String> elements = new ArrayList<>();

        @Override
        public void collect(String element) {
            elements.add(element);
        }

        @Override
        public void collectWithTimestamp(String element, long timestamp) {

        }

        @Override
        public void emitWatermark(Watermark mark) {

        }

        @Override
        public void markAsTemporarilyIdle() {

        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

        @Override
        public void close() {

        }
    }

    private static class TestConsumer implements Consumer<byte[]> {

        private final List<Message> messages = new ArrayList<>();

        private AtomicInteger currentMessage = new AtomicInteger();

        private final Map<MessageId, MessageId> acknowledgedIds = new ConcurrentHashMap<>();

        private TestConsumer(int numMessages) {
            messages.addAll(createMessages(0, numMessages));
        }

        private void reset() {
            currentMessage.set(0);
        }

        @Override
        public String getTopic() {
            return null;
        }

        @Override
        public String getSubscription() {
            return null;
        }

        @Override
        public void unsubscribe() throws PulsarClientException {

        }

        @Override
        public CompletableFuture<Void> unsubscribeAsync() {
            return null;
        }

        @Override
        public Message<byte[]> receive() throws PulsarClientException {
            return null;
        }

        public synchronized void addMessages(List<Message> messages) {
            this.messages.addAll(messages);
        }

        @Override
        public CompletableFuture<Message<byte[]>> receiveAsync() {
            return null;
        }

        @Override
        public Message<byte[]> receive(int i, TimeUnit timeUnit) throws PulsarClientException {
            synchronized (this) {
                if (currentMessage.get() == messages.size()) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        System.out.println("no more messages sleeping index: " + currentMessage.get());
                    }
                    return null;
                }
                return messages.get(currentMessage.getAndIncrement());
            }
        }

        @Override
<<<<<<< HEAD
=======
        public Messages<byte[]> batchReceive() throws PulsarClientException {
            return null;
        }

        @Override
        public CompletableFuture<Messages<byte[]>> batchReceiveAsync() {
            return null;
        }

        @Override
>>>>>>> f773c602c... Test pr 10 (#27)
        public void acknowledge(Message<?> message) throws PulsarClientException {

        }

        @Override
        public void acknowledge(MessageId messageId) throws PulsarClientException {

        }

        @Override
<<<<<<< HEAD
=======
        public void acknowledge(Messages<?> messages) throws PulsarClientException {

        }

        @Override
        public void acknowledge(List<MessageId> messageIdList) throws PulsarClientException {

        }

        @Override
        public void negativeAcknowledge(Message<?> message) {
        }

        @Override
        public void negativeAcknowledge(MessageId messageId) {
        }

        @Override
        public void negativeAcknowledge(Messages<?> messages) {

        }

        @Override
>>>>>>> f773c602c... Test pr 10 (#27)
        public void acknowledgeCumulative(Message<?> message) throws PulsarClientException {

        }

        @Override
        public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException {
            acknowledgedIds.put(messageId, messageId);
        }

        @Override
        public CompletableFuture<Void> acknowledgeAsync(Message<?> message) {
            return null;
        }

        @Override
        public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
            acknowledgedIds.put(messageId, messageId);
            return CompletableFuture.completedFuture(null);
        }

        @Override
<<<<<<< HEAD
=======
        public CompletableFuture<Void> acknowledgeAsync(Messages<?> messages) {
            return null;
        }

        @Override
        public CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList) {
            return null;
        }

        @Override
>>>>>>> f773c602c... Test pr 10 (#27)
        public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message) {
            return null;
        }

        @Override
        public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
            return null;
        }

        @Override
        public ConsumerStats getStats() {
            return null;
        }

        @Override
        public void close() throws PulsarClientException {

        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return null;
        }

        @Override
        public boolean hasReachedEndOfTopic() {
            return false;
        }

        @Override
        public void redeliverUnacknowledgedMessages() {

        }

        @Override
        public void seek(MessageId messageId) throws PulsarClientException {

        }

        @Override
<<<<<<< HEAD
=======
        public void seek(long timestamp) throws PulsarClientException {

        }

        @Override
>>>>>>> f773c602c... Test pr 10 (#27)
        public CompletableFuture<Void> seekAsync(MessageId messageId) {
            return null;
        }

        @Override
<<<<<<< HEAD
=======
        public CompletableFuture<Void> seekAsync(long timestamp) {
            return null;
        }

        @Override
>>>>>>> f773c602c... Test pr 10 (#27)
        public boolean isConnected() {
            return true;
        }

        @Override
        public String getConsumerName() {
            return "test-consumer-0";
        }

        @Override
        public void pause() {
        }

        @Override
        public void resume() {
        }
<<<<<<< HEAD
=======

        @Override
        public MessageId getLastMessageId() throws PulsarClientException {
            return null;
        }

        @Override
        public CompletableFuture<MessageId> getLastMessageIdAsync() {
            return null;
        }

        @Override
        public void reconsumeLater(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException {
            
        }

        @Override
        public void reconsumeLater(Messages<?> messages, long delayTime, TimeUnit unit) throws PulsarClientException {
            
        }

        @Override
        public void reconsumeLaterCumulative(Message<?> message, long delayTime, TimeUnit unit)
                throws PulsarClientException {
            
        }

        @Override
        public CompletableFuture<Void> reconsumeLaterAsync(Message<?> message, long delayTime, TimeUnit unit) {
            return null;
        }

        @Override
        public CompletableFuture<Void> reconsumeLaterAsync(Messages<?> messages, long delayTime, TimeUnit unit) {
            return null;
        }

        @Override
        public CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> message, long delayTime,
                TimeUnit unit) {
            return null;
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private static List<Message> createMessages(int startIndex, int numMessages) {
        final List<Message> messages = new ArrayList<>();
        for (int i = startIndex; i < (startIndex + numMessages); ++i) {
            String content = "message-" + i;
            messages.add(createMessage(content, createMessageId(1, i + 1, 1)));
        }
        return messages;
    }

    private static Message<byte[]> createMessage(String content, String messageId) {
        return new MessageImpl<byte[]>("my-topic", messageId, Collections.emptyMap(),
<<<<<<< HEAD
                                       Unpooled.wrappedBuffer(content.getBytes()), Schema.BYTES);
=======
                                       content.getBytes(), Schema.BYTES, PulsarApi.MessageMetadata.newBuilder());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private static String createMessageId(long ledgerId, long entryId, long partitionIndex) {
        return String.format("%d:%d:%d", ledgerId, entryId, partitionIndex);
    }
}
