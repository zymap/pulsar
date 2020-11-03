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
 * A schema for 'Byte'.
 */
<<<<<<< HEAD
public class ByteSchema implements Schema<Byte> {
=======
public class ByteSchema extends AbstractSchema<Byte> {

    private static final ByteSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfo()
            .setName("INT8")
            .setType(SchemaType.INT8)
            .setSchema(new byte[0]);
        INSTANCE = new ByteSchema();
    }
>>>>>>> f773c602c... Test pr 10 (#27)

    public static ByteSchema of() {
        return INSTANCE;
    }

<<<<<<< HEAD
    private static final ByteSchema INSTANCE = new ByteSchema();
    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
        .setName("INT8")
        .setType(SchemaType.INT8)
        .setSchema(new byte[0]);

=======
>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public void validate(byte[] message) {
        if (message.length != 1) {
            throw new SchemaSerializationException("Size of data received by ByteSchema is not 1");
        }
    }

    @Override
<<<<<<< HEAD
=======
    public void validate(ByteBuf message) {
        if (message.readableBytes() != 1) {
            throw new SchemaSerializationException("Size of data received by ByteSchema is not 1");
        }
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public byte[] encode(Byte message) {
        if (null == message) {
            return null;
        } else {
            return new byte[]{message};
        }
    }

    @Override
    public Byte decode(byte[] bytes) {
        if (null == bytes) {
            return null;
        }
        validate(bytes);
        return bytes[0];
    }

    @Override
<<<<<<< HEAD
=======
    public Byte decode(ByteBuf byteBuf) {
        if (null == byteBuf) {
            return null;
        }
        validate(byteBuf);
        return byteBuf.getByte(0);
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
