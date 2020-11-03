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

/**
 * When creating a consumer, if the subscription does not exist, a new subscription will be created. By default the
 * subscription will be created at the end of the topic. See
 * {@link #subscriptionInitialPosition(SubscriptionInitialPosition)} to configure the initial position behavior.
 *
 */
public enum SubscriptionInitialPosition {
    /**
<<<<<<< HEAD
     * the latest position which means the start consuming position will be the last message
=======
     * The latest position which means the start consuming position will be the last message.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    Latest(0),

    /**
<<<<<<< HEAD
     * the earliest position which means the start consuming position will be the first message
     */
    Earliest(1),
    ;
=======
     * The earliest position which means the start consuming position will be the first message.
     */
    Earliest(1);
>>>>>>> f773c602c... Test pr 10 (#27)


    private final int value;

    SubscriptionInitialPosition(int value) {
        this.value = value;
    }

<<<<<<< HEAD
    public final int getValue() { return value; }
=======
    public final int getValue() {
        return value;
    }
>>>>>>> f773c602c... Test pr 10 (#27)

}
