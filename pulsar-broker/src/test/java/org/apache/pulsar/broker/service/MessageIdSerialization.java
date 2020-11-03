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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;

<<<<<<< HEAD
import java.io.IOException;

=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.Test;

<<<<<<< HEAD
@Test
public class MessageIdSerialization {

    @Test
    void testProtobufSerialization1() throws Exception {
=======
public class MessageIdSerialization {

    @Test
    public void testProtobufSerialization1() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        MessageId id = new MessageIdImpl(1, 2, 3);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
    }

    @Test
<<<<<<< HEAD
    void testProtobufSerialization2() throws Exception {
=======
    public void testProtobufSerialization2() throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)
        MessageId id = new MessageIdImpl(1, 2, -1);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
    }

    @Test(expectedExceptions = NullPointerException.class)
<<<<<<< HEAD
    void testProtobufSerializationNull() throws Exception {
        MessageId.fromByteArray(null);
    }

    @Test(expectedExceptions = IOException.class)
=======
    public void testProtobufSerializationNull() throws Exception {
        MessageId.fromByteArray(null);
    }

    @Test(expectedExceptions = RuntimeException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    void testProtobufSerializationEmpty() throws Exception {
        MessageId.fromByteArray(new byte[0]);
    }
}
