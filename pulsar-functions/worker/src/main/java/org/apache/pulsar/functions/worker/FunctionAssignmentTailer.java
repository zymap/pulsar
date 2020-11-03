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
package org.apache.pulsar.functions.worker;

<<<<<<< HEAD
import java.io.IOException;
import java.util.function.Function;

=======
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
<<<<<<< HEAD
import org.apache.pulsar.functions.proto.Function.Assignment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FunctionAssignmentTailer
    implements java.util.function.Consumer<Message<byte[]>>, Function<Throwable, Void>, AutoCloseable {

        private final FunctionRuntimeManager functionRuntimeManager;
        private final Reader<byte[]> reader;

    public FunctionAssignmentTailer(FunctionRuntimeManager functionRuntimeManager, Reader<byte[]> reader)
            throws PulsarClientException {
        this.functionRuntimeManager = functionRuntimeManager;

        this.reader = reader;
    }

    public void start() {
        receiveOne();
    }

    private void receiveOne() {
        reader.readNextAsync()
                .thenAccept(this)
                .exceptionally(this);
    }

    @Override
    public void close() {
        log.info("Stopping function state consumer");
        try {
            reader.close();
        } catch (IOException e) {
            log.error("Failed to stop function state consumer", e);
        }
        log.info("Stopped function state consumer");
    }

    public void processAssignment(Message<byte[]> msg) {
        if(msg.getData()==null || (msg.getData().length==0)) {
            log.info("Received assignment delete: {}", msg.getKey());
            this.functionRuntimeManager.deleteAssignment(msg.getKey());
        } else {
            Assignment assignment;
            try {
                assignment = Assignment.parseFrom(msg.getData());
            } catch (IOException e) {
                log.error("[{}] Received bad assignment update at message {}", reader.getTopic(), msg.getMessageId(),
                        e);
                // TODO: find a better way to handle bad request
                throw new RuntimeException(e);
            }
            log.info("Received assignment update: {}", assignment);
            this.functionRuntimeManager.processAssignment(assignment);
        }
    }

    @Override
    public void accept(Message<byte[]> msg) {
        processAssignment(msg);
        // receive next request
        receiveOne();
    }

    @Override
    public Void apply(Throwable cause) {
        log.error("Failed to retrieve messages from assignment update topic", cause);
        // TODO: find a better way to handle consumer functions
        throw new RuntimeException(cause);
=======
import org.apache.pulsar.client.api.ReaderBuilder;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for reading assignments from the 'assignments' functions internal topic.
 * Only functions worker leader writes to the topic while other workers read from the topic.
 * When a worker become a leader, the worker will read to the end of the assignments topic and close its reader to the topic.
 * Then the worker and new leader will be in charge of computing new assignments when necessary.
 * The leader does not need to listen to the assignments topic because it can just update its in memory assignments map directly
 * after it computes a new scheduling.  When a worker loses leadership, the worker is start reading from the assignments topic again.
 */
@Slf4j
public class FunctionAssignmentTailer implements AutoCloseable {

    private final FunctionRuntimeManager functionRuntimeManager;
    private final ReaderBuilder readerBuilder;
    private final WorkerConfig workerConfig;
    private final ErrorNotifier errorNotifier;
    private Reader<byte[]> reader;
    private volatile boolean isRunning = false;
    private volatile boolean exitOnEndOfTopic = false;
    private CompletableFuture<Void> exitFuture;
    private Thread tailerThread;

    @Getter
    private MessageId lastMessageId = null;
    
    public FunctionAssignmentTailer(
            FunctionRuntimeManager functionRuntimeManager,
            ReaderBuilder readerBuilder,
            WorkerConfig workerConfig,
            ErrorNotifier errorNotifier) {
        this.functionRuntimeManager = functionRuntimeManager;
        this.exitFuture = new CompletableFuture<>();
        this.readerBuilder = readerBuilder;
        this.workerConfig = workerConfig;
        this.errorNotifier = errorNotifier;
    }

    public synchronized CompletableFuture<Void> triggerReadToTheEndAndExit() {
        exitOnEndOfTopic = true;
        return this.exitFuture;
    }

    public void startFromMessage(MessageId startMessageId) throws PulsarClientException {
        if (!isRunning) {
            isRunning = true;
            if (reader == null) {
                reader = createReader(startMessageId);
            }
            if (tailerThread == null || !tailerThread.isAlive()) {
                tailerThread = getTailerThread();
            }
            tailerThread.start();
        }
    }

    public synchronized void start() throws PulsarClientException {
        MessageId startMessageId = lastMessageId == null ? MessageId.earliest : lastMessageId;
        startFromMessage(startMessageId);
    }

    @Override
    public synchronized void close() {
        log.info("Closing function assignment tailer");
        try {
            isRunning = false;

            if (tailerThread != null) {
                while (true) {
                    tailerThread.interrupt();

                    try {
                        tailerThread.join(5000, 0);
                    } catch (InterruptedException e) {
                        log.warn("Waiting for assignment tailer thread to stop is interrupted", e);
                    }

                    if (tailerThread.isAlive()) {
                        log.warn("Assignment tailer thread is still alive.  Will attempt to interrupt again.");
                    } else {
                        break;
                    }
                }
                tailerThread = null;

                // complete exit future to be safe
                exitFuture.complete(null);
                // reset the future
                exitFuture = new CompletableFuture<>();
            }
            if (reader != null) {
                reader.close();
                reader = null;
            }

            exitOnEndOfTopic = false;
        } catch (IOException e) {
            log.error("Failed to stop function assignment tailer", e);
        }
    }
    
    private Reader<byte[]> createReader(MessageId startMessageId) throws PulsarClientException {
        log.info("Assignment tailer will start reading from message id {}", startMessageId);

        return WorkerUtils.createReader(
                readerBuilder,
                workerConfig.getWorkerId() + "-function-assignment-tailer",
                workerConfig.getFunctionAssignmentTopic(),
                startMessageId);
    }

    private Thread getTailerThread() {
        Thread t = new Thread(() -> {
            while (isRunning) {
                try {
                    Message<byte[]> msg = reader.readNext(1, TimeUnit.SECONDS);
                    if (msg == null) {
                        if (exitOnEndOfTopic && !reader.hasMessageAvailable()) {
                            break;
                        }
                    } else {
                        functionRuntimeManager.processAssignmentMessage(msg);
                        // keep track of last message id
                        lastMessageId = msg.getMessageId();
                    }
                } catch (Throwable th) {
                    if (isRunning) {
                        log.error("Encountered error in assignment tailer", th);
                        // trigger fatal error
                        isRunning = false;
                        errorNotifier.triggerError(th);
                    } else {
                        if (!(th instanceof InterruptedException || th.getCause() instanceof InterruptedException)) {
                            log.warn("Encountered error when assignment tailer is not running", th);
                        }
                    }
                }
            }
            log.info("assignment tailer thread exiting");
            exitFuture.complete(null);
        });
        t.setName("assignment-tailer-thread");
        return t;
    }

    Thread getThread() {
        return tailerThread;
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}
