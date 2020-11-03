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
package org.apache.pulsar.client.api;

<<<<<<< HEAD
/**
 * A plugin interface that allows you to intercept (and possibly mutate)
 * messages received by the consumer.
 * <p>
 * A primary use case is to hook into consumer applications for custom
 * monitoring, logging, etc.
 * <p>
 * Exceptions thrown by interceptor methods will be caught, logged, but
=======
import java.util.Set;

/**
 * A plugin interface that allows you to intercept (and possibly mutate)
 * messages received by the consumer.
 *
 * <p>A primary use case is to hook into consumer applications for custom
 * monitoring, logging, etc.
 *
 * <p>Exceptions thrown by interceptor methods will be caught, logged, but
>>>>>>> f773c602c... Test pr 10 (#27)
 * not propagated further.
 */
public interface ConsumerInterceptor<T> extends AutoCloseable {

    /**
     * Close the interceptor.
     */
    void close();

    /**
     * This is called just before the message is returned by
     * {@link Consumer#receive()}, {@link MessageListener#received(Consumer,
     * Message)} or the {@link java.util.concurrent.CompletableFuture} returned by
     * {@link Consumer#receiveAsync()} completes.
<<<<<<< HEAD
     * <p>
     * This method is allowed to modify message, in which case the new message
     * will be returned.
     * <p>
     * Any exception thrown by this method will be caught by the caller, logged,
     * but not propagated to client.
     * <p>
     * Since the consumer may run multiple interceptors, a particular
=======
     *
     * <p>This method is allowed to modify message, in which case the new message
     * will be returned.
     *
     * <p>Any exception thrown by this method will be caught by the caller, logged,
     * but not propagated to client.
     *
     * <p>Since the consumer may run multiple interceptors, a particular
>>>>>>> f773c602c... Test pr 10 (#27)
     * interceptor's
     * <tt>beforeConsume</tt> callback will be called in the order specified by
     * {@link ConsumerBuilder#intercept(ConsumerInterceptor[])}. The first
     * interceptor in the list gets the consumed message, the following
     * interceptor will be passed
     * the message returned by the previous interceptor, and so on. Since
     * interceptors are allowed to modify message, interceptors may potentially
     * get the messages already modified by other interceptors. However building a
     * pipeline of mutable
     * interceptors that depend on the output of the previous interceptor is
     * discouraged, because of potential side-effects caused by interceptors
     * potentially failing to modify the message and throwing an exception.
     * if one of interceptors in the list throws an exception from
     * <tt>beforeConsume</tt>, the exception is caught, logged,
     * and the next interceptor is called with the message returned by the last
     * successful interceptor in the list, or otherwise the original consumed
     * message.
     *
     * @param consumer the consumer which contains the interceptor
     * @param message the message to be consumed by the client.
     * @return message that is either modified by the interceptor or same message
     *         passed into the method.
     */
    Message<T> beforeConsume(Consumer<T> consumer, Message<T> message);

    /**
     * This is called consumer sends the acknowledgment to the broker.
     *
     * <p>Any exception thrown by this method will be ignored by the caller.
     *
     * @param consumer the consumer which contains the interceptor
     * @param messageId message to ack, null if acknowledge fail.
     * @param exception the exception on acknowledge.
     */
    void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception);

    /**
     * This is called consumer send the cumulative acknowledgment to the broker.
     *
     * <p>Any exception thrown by this method will be ignored by the caller.
     *
     * @param consumer the consumer which contains the interceptor
     * @param messageId message to ack, null if acknowledge fail.
     * @param exception the exception on acknowledge.
     */
    void onAcknowledgeCumulative(Consumer<T> consumer, MessageId messageId, Throwable exception);
<<<<<<< HEAD
=======

    /**
     * This method will be called when a redelivery from a negative acknowledge occurs.
     *
     * <p>Any exception thrown by this method will be ignored by the caller.
     *
     * @param consumer the consumer which contains the interceptor
     * @param messageIds message to ack, null if acknowledge fail.
     */
    void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds);

    /**
     * This method will be called when a redelivery from an acknowledge timeout occurs.
     *
     * <p>Any exception thrown by this method will be ignored by the caller.
     *
     * @param consumer the consumer which contains the interceptor
     * @param messageIds message to ack, null if acknowledge fail.
     */
    void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds);
>>>>>>> f773c602c... Test pr 10 (#27)
}
