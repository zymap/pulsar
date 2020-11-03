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
package org.apache.pulsar.functions.instance;

<<<<<<< HEAD
=======
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
>>>>>>> f773c602c... Test pr 10 (#27)
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
<<<<<<< HEAD
import org.apache.pulsar.functions.proto.InstanceCommunication;
=======
>>>>>>> f773c602c... Test pr 10 (#27)

import java.util.Map;

/**
 * This is the Java Instance. This is started by the runtimeSpawner using the JavaInstanceClient
 * program if invoking via a process based invocation or using JavaInstance using a thread
 * based invocation.
 */
@Slf4j
public class JavaInstance implements AutoCloseable {

    @Getter(AccessLevel.PACKAGE)
    private final ContextImpl context;
    private Function function;
    private java.util.function.Function javaUtilFunction;

<<<<<<< HEAD
    public JavaInstance(ContextImpl contextImpl, Object userClassObject) {

        this.context = contextImpl;
=======
    // for Async function max out standing items
    private final InstanceConfig instanceConfig;
    private final Executor executor;
    @Getter
    private final LinkedBlockingQueue<CompletableFuture<Void>> pendingAsyncRequests;

    public JavaInstance(ContextImpl contextImpl, Object userClassObject, InstanceConfig instanceConfig) {

        this.context = contextImpl;
        this.instanceConfig = instanceConfig;
        this.executor = Executors.newSingleThreadExecutor();
        this.pendingAsyncRequests = new LinkedBlockingQueue<>(this.instanceConfig.getMaxPendingAsyncRequests());
>>>>>>> f773c602c... Test pr 10 (#27)

        // create the functions
        if (userClassObject instanceof Function) {
            this.function = (Function) userClassObject;
        } else {
            this.javaUtilFunction = (java.util.function.Function) userClassObject;
        }
    }

<<<<<<< HEAD
    public JavaExecutionResult handleMessage(Record<?> record, Object input) {
        if (context != null) {
            context.setCurrentMessageContext(record);
        }
        JavaExecutionResult executionResult = new JavaExecutionResult();
        try {
            Object output;
=======
    public CompletableFuture<JavaExecutionResult> handleMessage(Record<?> record, Object input) {
        if (context != null) {
            context.setCurrentMessageContext(record);
        }

        final CompletableFuture<JavaExecutionResult> future = new CompletableFuture<>();
        JavaExecutionResult executionResult = new JavaExecutionResult();

        final Object output;

        try {
>>>>>>> f773c602c... Test pr 10 (#27)
            if (function != null) {
                output = function.process(input, context);
            } else {
                output = javaUtilFunction.apply(input);
            }
<<<<<<< HEAD
            executionResult.setResult(output);
        } catch (Exception ex) {
            executionResult.setUserException(ex);
        }
        return executionResult;
=======
        } catch (Exception ex) {
            executionResult.setUserException(ex);
            future.complete(executionResult);
            return future;
        }

        if (output instanceof CompletableFuture) {
            // Function is in format: Function<I, CompletableFuture<O>>
            try {
                pendingAsyncRequests.put((CompletableFuture) output);
            } catch (InterruptedException ie) {
                log.warn("Exception while put Async requests", ie);
                executionResult.setUserException(ie);
                future.complete(executionResult);
                return future;
            }

            ((CompletableFuture) output).whenCompleteAsync((obj, throwable) -> {
                if (log.isDebugEnabled()) {
                    log.debug("Got result async: object: {}, throwable: {}", obj, throwable);
                }

                if (throwable != null) {
                    executionResult.setUserException(new Exception((Throwable)throwable));
                    pendingAsyncRequests.remove(output);
                    future.complete(executionResult);
                    return;
                }
                executionResult.setResult(obj);
                pendingAsyncRequests.remove(output);
                future.complete(executionResult);
            }, executor);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Got result: object: {}", output);
            }
            executionResult.setResult(output);
            future.complete(executionResult);
        }

        return future;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public void close() {
<<<<<<< HEAD
=======
        context.close();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public Map<String, Double> getAndResetMetrics() {
        return context.getAndResetMetrics();
    }

    public void resetMetrics() {
        context.resetMetrics();
    }

    public Map<String, Double> getMetrics() {
        return context.getMetrics();
    }
}
