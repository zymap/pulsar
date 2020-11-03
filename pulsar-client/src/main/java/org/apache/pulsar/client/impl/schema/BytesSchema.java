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
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for bytes array.
 */
<<<<<<< HEAD
public class BytesSchema implements Schema<byte[]> {
=======
public class BytesSchema extends AbstractSchema<byte[]> {

    private static final BytesSchema INSTANCE;
    private static final SchemaInfo SCHEMA_INFO;

    static {
        SCHEMA_INFO = new SchemaInfo()
            .setName("Bytes")
            .setType(SchemaType.BYTES)
            .setSchema(new byte[0]);
        INSTANCE = new BytesSchema();
    }
>>>>>>> f773c602c... Test pr 10 (#27)

    public static BytesSchema of() {
        return INSTANCE;
    }

<<<<<<< HEAD
    private static final BytesSchema INSTANCE = new BytesSchema();
    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
        .setName("Bytes")
        .setType(SchemaType.BYTES)
        .setSchema(new byte[0]);

=======
>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public byte[] encode(byte[] message) {
        return message;
    }

    @Override
    public byte[] decode(byte[] bytes) {
        return bytes;
    }

    @Override
<<<<<<< HEAD
=======
    public byte[] decode(ByteBuf byteBuf) {
        if (byteBuf == null) {
            return null;
        }
        int size = byteBuf.readableBytes();
        byte[] bytes = new byte[size];

        byteBuf.readBytes(bytes, 0, size);
        return bytes;
    }

    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
