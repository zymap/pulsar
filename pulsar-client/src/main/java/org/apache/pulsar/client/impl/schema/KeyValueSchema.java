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

import static com.google.common.base.Preconditions.checkArgument;

<<<<<<< HEAD
import java.nio.ByteBuffer;

import lombok.Getter;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
=======
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * [Key, Value] pair schema definition
 */
<<<<<<< HEAD
public class KeyValueSchema<K, V> implements Schema<KeyValue<K, V>> {
=======
@Slf4j
public class KeyValueSchema<K, V> implements Schema<KeyValue<K, V>> {

>>>>>>> f773c602c... Test pr 10 (#27)
    @Getter
    private final Schema<K> keySchema;
    @Getter
    private final Schema<V> valueSchema;

<<<<<<< HEAD
    // schemaInfo combined by KeySchemaInfo and ValueSchemaInfo:
    //   [keyInfo.length][keyInfo][valueInfo.length][ValueInfo]
    private final SchemaInfo schemaInfo;
=======
    @Getter
    private final KeyValueEncodingType keyValueEncodingType;

    // schemaInfo combined by KeySchemaInfo and ValueSchemaInfo:
    //   [keyInfo.length][keyInfo][valueInfo.length][ValueInfo]
    private SchemaInfo schemaInfo;
    protected SchemaInfoProvider schemaInfoProvider;
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * Key Value Schema using passed in schema type, support JSON and AVRO currently.
     */
    public static <K, V> Schema<KeyValue<K, V>> of(Class<K> key, Class<V> value, SchemaType type) {
        checkArgument(SchemaType.JSON == type || SchemaType.AVRO == type);
        if (SchemaType.JSON == type) {
<<<<<<< HEAD
            return new KeyValueSchema<>(JSONSchema.of(key), JSONSchema.of(value));
        } else {
            // AVRO
            return new KeyValueSchema<>(AvroSchema.of(key), AvroSchema.of(value));
        }
    }

    public KeyValueSchema(Schema<K> keySchema,
                          Schema<V> valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;

        // set schemaInfo
        this.schemaInfo = new SchemaInfo()
            .setName("KeyValue")
            .setType(SchemaType.KEY_VALUE);

        byte[] keySchemaInfo = keySchema.getSchemaInfo().getSchema();
        byte[] valueSchemaInfo = valueSchema.getSchemaInfo().getSchema();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keySchemaInfo.length + 4 + valueSchemaInfo.length);
        byteBuffer.putInt(keySchemaInfo.length).put(keySchemaInfo)
            .putInt(valueSchemaInfo.length).put(valueSchemaInfo);
        this.schemaInfo.setSchema(byteBuffer.array());
    }

    // encode as bytes: [key.length][key.bytes][value.length][value.bytes]
    public byte[] encode(KeyValue<K, V> message) {
        byte[] keyBytes = keySchema.encode(message.getKey());
        byte[] valueBytes = valueSchema.encode(message.getValue());

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + valueBytes.length);
        byteBuffer.putInt(keyBytes.length).put(keyBytes).putInt(valueBytes.length).put(valueBytes);
        return byteBuffer.array();
    }

    public KeyValue<K, V> decode(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength = byteBuffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        byteBuffer.get(keyBytes);

        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuffer.get(valueBytes);

        return new KeyValue<>(keySchema.decode(keyBytes), valueSchema.decode(valueBytes));
=======
            return new KeyValueSchema<>(JSONSchema.of(key), JSONSchema.of(value), KeyValueEncodingType.INLINE);
        } else {
            // AVRO
            return new KeyValueSchema<>(AvroSchema.of(key), AvroSchema.of(value), KeyValueEncodingType.INLINE);
        }
    }


    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema, Schema<V> valueSchema) {
        return new KeyValueSchema<>(keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema,
                                                   Schema<V> valueSchema,
                                                   KeyValueEncodingType keyValueEncodingType) {
        return new KeyValueSchema<>(keySchema, valueSchema, keyValueEncodingType);
    }

    private static final Schema<KeyValue<byte[], byte[]>> KV_BYTES = new KeyValueSchema<>(
        BytesSchema.of(),
        BytesSchema.of());

    public static Schema<KeyValue<byte[], byte[]>> kvBytes() {
        return KV_BYTES;
    }

    @Override
    public boolean supportSchemaVersioning() {
        return keySchema.supportSchemaVersioning() || valueSchema.supportSchemaVersioning();
    }

    private KeyValueSchema(Schema<K> keySchema,
                           Schema<V> valueSchema) {
        this(keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    private KeyValueSchema(Schema<K> keySchema,
                           Schema<V> valueSchema,
                           KeyValueEncodingType keyValueEncodingType) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.keyValueEncodingType = keyValueEncodingType;
        this.schemaInfoProvider = new SchemaInfoProvider() {
            @Override
            public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
                return CompletableFuture.completedFuture(schemaInfo);
            }

            @Override
            public CompletableFuture<SchemaInfo> getLatestSchema() {
                return CompletableFuture.completedFuture(schemaInfo);
            }

            @Override
            public String getTopicName() {
                return "key-value-schema";
            }
        };
        // if either key schema or value schema requires fetching schema info,
        // we don't need to configure the key/value schema info right now.
        // defer configuring the key/value schema info until `configureSchemaInfo` is called.
        if (!requireFetchingSchemaInfo()) {
            configureKeyValueSchemaInfo();
        }
    }

    // encode as bytes: [key.length][key.bytes][value.length][value.bytes] or [value.bytes]
    public byte[] encode(KeyValue<K, V> message) {
        if (keyValueEncodingType != null && keyValueEncodingType == KeyValueEncodingType.INLINE) {
            return KeyValue.encode(
                message.getKey(),
                keySchema,
                message.getValue(),
                valueSchema
            );
        } else {
            if (message.getValue() == null) {
                return null;
            }
            return valueSchema.encode(message.getValue());
        }
    }

    public KeyValue<K, V> decode(byte[] bytes) {
        return decode(bytes, null);
    }

    public KeyValue<K, V> decode(byte[] bytes, byte[] schemaVersion) {
        if (this.keyValueEncodingType == KeyValueEncodingType.SEPARATED) {
            throw new SchemaSerializationException("This method cannot be used under this SEPARATED encoding type");
        }

        return KeyValue.decode(bytes, (keyBytes, valueBytes) -> decode(keyBytes, valueBytes, schemaVersion));
    }

    public KeyValue<K, V> decode(byte[] keyBytes, byte[] valueBytes, byte[] schemaVersion) {
        K k;
        if (keyBytes == null) {
            k = null;
        } else {
            if (keySchema.supportSchemaVersioning() && schemaVersion != null) {
                k = keySchema.decode(keyBytes, schemaVersion);
            } else {
                k = keySchema.decode(keyBytes);
            }
        }

        V v;
        if (valueBytes == null) {
            v = null;
        } else {
            if (valueSchema.supportSchemaVersioning() && schemaVersion != null) {
                v = valueSchema.decode(valueBytes, schemaVersion);
            } else {
                v = valueSchema.decode(valueBytes);
            }
        }
        return new KeyValue<>(k, v);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }
<<<<<<< HEAD
=======

    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        this.schemaInfoProvider = schemaInfoProvider;
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return keySchema.requireFetchingSchemaInfo() || valueSchema.requireFetchingSchemaInfo();
    }

    @Override
    public void configureSchemaInfo(String topicName,
                                    String componentName,
                                    SchemaInfo schemaInfo) {
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
        keySchema.configureSchemaInfo(topicName, "key", kvSchemaInfo.getKey());
        valueSchema.configureSchemaInfo(topicName, "value", kvSchemaInfo.getValue());
        configureKeyValueSchemaInfo();

        if (null == this.schemaInfo) {
            throw new RuntimeException(
                "No key schema info or value schema info : key = " + keySchema.getSchemaInfo()
                    + ", value = " + valueSchema.getSchemaInfo());
        }
    }

    @Override
    public Schema<KeyValue<K, V>> clone() {
        return KeyValueSchema.of(keySchema.clone(), valueSchema.clone(), keyValueEncodingType);
    }

    private void configureKeyValueSchemaInfo() {
        this.schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
            keySchema, valueSchema, keyValueEncodingType
        );

        this.keySchema.setSchemaInfoProvider(new SchemaInfoProvider() {
            @Override
            public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
                return schemaInfoProvider.getSchemaByVersion(schemaVersion)
                    .thenApply(si -> KeyValueSchemaInfo.decodeKeyValueSchemaInfo(si).getKey());
            }

            @Override
            public CompletableFuture<SchemaInfo> getLatestSchema() {
                return CompletableFuture.completedFuture(
                    ((AbstractStructSchema<K>) keySchema).schemaInfo);
            }

            @Override
            public String getTopicName() {
                return "key-schema";
            }
        });

        this.valueSchema.setSchemaInfoProvider(new SchemaInfoProvider() {
            @Override
            public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
                return schemaInfoProvider.getSchemaByVersion(schemaVersion)
                    .thenApply(si -> KeyValueSchemaInfo.decodeKeyValueSchemaInfo(si).getValue());
            }

            @Override
            public CompletableFuture<SchemaInfo> getLatestSchema() {
                return CompletableFuture.completedFuture(
                    ((AbstractStructSchema<V>) valueSchema).schemaInfo);
            }

            @Override
            public String getTopicName() {
                return "value-schema";
            }
        });
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
