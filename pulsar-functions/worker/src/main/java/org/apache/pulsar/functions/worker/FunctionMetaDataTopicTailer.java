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

import java.io.IOException;
<<<<<<< HEAD
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.functions.proto.Request.ServiceRequest;

@Slf4j
public class FunctionMetaDataTopicTailer
        implements java.util.function.Consumer<Message<byte[]>>, Function<Throwable, Void>, AutoCloseable {

    private final FunctionMetaDataManager functionMetaDataManager;
    private final Reader<byte[]> reader;

    public FunctionMetaDataTopicTailer(FunctionMetaDataManager functionMetaDataManager,
                                       Reader<byte[]> reader)
            throws PulsarClientException {
        this.functionMetaDataManager = functionMetaDataManager;
        this.reader = reader;
    }

    public void start() {
        receiveOne();
    }

    private void receiveOne() {
        reader.readNextAsync()
                .thenAccept(this)
                .exceptionally(this);
=======
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
public class FunctionMetaDataTopicTailer
        implements Runnable, AutoCloseable {

    private final FunctionMetaDataManager functionMetaDataManager;
    @Getter
    private final Reader<byte[]> reader;
    private final Thread tailerThread;
    private volatile boolean isRunning;
    private ErrorNotifier errorNotifier;
    private volatile boolean exitOnEndOfTopic;
    private CompletableFuture<Void> exitFuture = new CompletableFuture<>();

    public FunctionMetaDataTopicTailer(FunctionMetaDataManager functionMetaDataManager,
                                       ReaderBuilder readerBuilder, WorkerConfig workerConfig,
                                       MessageId lastMessageSeen,
                                       ErrorNotifier errorNotifier)
            throws PulsarClientException {
        this.functionMetaDataManager = functionMetaDataManager;
        this.reader = createReader(workerConfig, readerBuilder, lastMessageSeen);
        tailerThread = new Thread(this);
        tailerThread.setName("function-metadata-tailer-thread");
        this.errorNotifier = errorNotifier;
        exitOnEndOfTopic = false;
    }

    public void start() {
        isRunning = true;
        tailerThread.start();
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                Message<byte[]> msg = reader.readNext(1, TimeUnit.SECONDS);
                if (msg == null) {
                    if (exitOnEndOfTopic && !reader.hasMessageAvailable()) {
                        break;
                    }
                } else {
                    functionMetaDataManager.processMetaDataTopicMessage(msg);
                }
            } catch (Throwable th) {
                if (isRunning) {
                    log.error("Encountered error in metadata tailer", th);
                    // trigger fatal error
                    isRunning = false;
                    errorNotifier.triggerError(th);
                } else {
                    if (!(th instanceof InterruptedException || th.getCause() instanceof InterruptedException)) {
                        log.warn("Encountered error when metadata tailer is not running", th);
                    }
                }
            }
        }
        log.info("metadata tailer thread exiting");
        exitFuture.complete(null);
    }

    public CompletableFuture<Void> stopWhenNoMoreMessages() {
        exitOnEndOfTopic = true;
        return exitFuture;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public void close() {
<<<<<<< HEAD
        log.info("Stopping function state consumer");
        try {
            reader.close();
        } catch (IOException e) {
            log.error("Failed to stop function state consumer", e);
        }
        log.info("Stopped function state consumer");
    }

    public void processRequest(Message<byte[]> msg) {
        ServiceRequest serviceRequest;

        try {
            serviceRequest = ServiceRequest.parseFrom(msg.getData());
        } catch (IOException e) {
            log.error("Received bad service request at message {}", msg.getMessageId(), e);
            // TODO: find a better way to handle bad request
            throw new RuntimeException(e);
        }
        if (log.isDebugEnabled()) {
            log.debug("Received Service Request: {}", serviceRequest);
        }

        this.functionMetaDataManager.processRequest(msg.getMessageId(), serviceRequest);
    }

    @Override
    public void accept(Message<byte[]> msg) {

        processRequest(msg);
        // receive next request
        receiveOne();
    }

    @Override
    public Void apply(Throwable cause) {
        log.error("Failed to retrieve messages from function state topic", cause);
        // TODO: find a better way to handle consumer functions
        throw new RuntimeException(cause);
=======
        log.info("Stopping function metadata tailer");
        try {
            isRunning = false;
            while (true) {
                tailerThread.interrupt();
                try {
                    tailerThread.join(5000, 0);
                } catch (InterruptedException e) {
                    log.warn("Waiting for metadata tailer thread to stop is interrupted", e);
                }

                if (tailerThread.isAlive()) {
                    log.warn("metadata tailer thread is still alive.  Will attempt to interrupt again.");
                } else {
                    break;
                }
            }

            reader.close();
        } catch (IOException e) {
            log.error("Failed to stop function metadata tailer", e);
        }
        log.info("Stopped function metadata tailer");
    }

    public static Reader createReader(WorkerConfig workerConfig, ReaderBuilder readerBuilder,
                                      MessageId startMessageId) throws PulsarClientException {
        ReaderBuilder builder = readerBuilder
                .topic(workerConfig.getFunctionMetadataTopic())
                .startMessageId(startMessageId)
                .readerName(workerConfig.getWorkerId() + "-function-metadata-tailer")
                .subscriptionRolePrefix(workerConfig.getWorkerId() + "-function-metadata-tailer");
        if (workerConfig.getUseCompactedMetadataTopic()) {
            builder = builder.readCompacted(true);
        }
        return builder.create();
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}
