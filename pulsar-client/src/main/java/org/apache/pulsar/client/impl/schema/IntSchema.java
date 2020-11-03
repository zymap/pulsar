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
package org.apache.pulsar.client.impl.schema;

<<<<<<< HEAD
import org.apache.pulsar.client.api.Schema;
=======
import io.netty.buffer.ByteBuf;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for `Integer`.
 */
<<<<<<< HEAD
public class IntSchema implements Schema<Integer> {
=======
public class IntSchema extends AbstractSchema<Integer> {

    private static final IntSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfo()
            .setName("INT32")
            .setType(SchemaType.INT32)
            .setSchema(new byte[0]);
        INSTANCE = new IntSchema();
    }
>>>>>>> f773c602c... Test pr 10 (#27)

    public static IntSchema of() {
        return INSTANCE;
    }

<<<<<<< HEAD
    private static final IntSchema INSTANCE = new IntSchema();
    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
        .setName("INT32")
        .setType(SchemaType.INT32)
        .setSchema(new byte[0]);

=======
>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public void validate(byte[] message) {
        if (message.length != 4) {
            throw new SchemaSerializationException("Size of data received by IntSchema is not 4");
        }
    }

    @Override
<<<<<<< HEAD
=======
    public void validate(ByteBuf message) {
        if (message.readableBytes() != 4) {
            throw new SchemaSerializationException("Size of data received by IntSchema is not 4");
        }
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public byte[] encode(Integer message) {
        if (null == message) {
            return null;
        } else {
            return new byte[] {
                (byte) (message >>> 24),
                (byte) (message >>> 16),
                (byte) (message >>> 8),
                message.byteValue()
            };
        }
    }

    @Override
    public Integer decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        validate(bytes);
        int value = 0;
        for (byte b : bytes) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    @Override
<<<<<<< HEAD
=======
    public Integer decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        }
        validate(byteBuf);
        int value = 0;

        for (int i = 0; i < 4; i++) {
            value <<= 8;
            value |= byteBuf.getByte(i) & 0xFF;
        }

        return value;
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
