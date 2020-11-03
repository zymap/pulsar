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

import lombok.extern.slf4j.Slf4j;
<<<<<<< HEAD
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@Slf4j
public class AvroSchema<T> implements Schema<T> {

    private SchemaInfo schemaInfo;
    private org.apache.avro.Schema schema;
    private ReflectDatumWriter<T> datumWriter;
    private ReflectDatumReader<T> reader;
    private BinaryEncoder encoder;
    private ByteArrayOutputStream byteArrayOutputStream;

    private static final ThreadLocal<BinaryDecoder> decoders =
            new ThreadLocal<>();

    private AvroSchema(org.apache.avro.Schema schema,
                       Map<String, String> properties) {
        this.schema = schema;

        this.schemaInfo = new SchemaInfo();
        this.schemaInfo.setName("");
        this.schemaInfo.setProperties(properties);
        this.schemaInfo.setType(SchemaType.AVRO);
        this.schemaInfo.setSchema(this.schema.toString().getBytes());

        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, this.encoder);
        this.datumWriter = new ReflectDatumWriter<>(this.schema);
        this.reader = new ReflectDatumReader<>(this.schema);
    }

    @Override
    public synchronized byte[] encode(T message) {
        try {
            datumWriter.write(message, this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            BinaryDecoder decoderFromCache = decoders.get();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, decoderFromCache);
            if (decoderFromCache == null) {
                decoders.set(decoder);
            }
            return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, decoder));
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }

    private static <T> org.apache.avro.Schema createAvroSchema(Class<T> pojo) {
        return ReflectData.AllowNull.get().getSchema(pojo);
    }

    public static <T> AvroSchema<T> of(Class<T> pojo) {
        return new AvroSchema<>(createAvroSchema(pojo), Collections.emptyMap());
    }

    public static <T> AvroSchema<T> of(Class<T> pojo, Map<String, String> properties) {
        return new AvroSchema<>(createAvroSchema(pojo), properties);
=======
import org.apache.avro.Conversions;
import org.apache.avro.data.JodaTimeConversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.reader.AvroReader;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An AVRO schema implementation.
 */
@Slf4j
public class AvroSchema<T> extends StructSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchema.class);

    private ClassLoader pojoClassLoader;

    private AvroSchema(SchemaInfo schemaInfo, ClassLoader pojoClassLoader) {
        super(schemaInfo);
        this.pojoClassLoader = pojoClassLoader;
        boolean jsr310ConversionEnabled = getJsr310ConversionEnabledFromSchemaInfo(schemaInfo);
        setReader(new AvroReader<>(schema, pojoClassLoader, jsr310ConversionEnabled));
        setWriter(new AvroWriter<>(schema, jsr310ConversionEnabled));
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public Schema<T> clone() {
        Schema<T> schema = new AvroSchema<>(schemaInfo, pojoClassLoader);
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        return schema;
    }

    public static <T> AvroSchema<T> of(SchemaDefinition<T> schemaDefinition) {
        ClassLoader pojoClassLoader = null;
        if (schemaDefinition.getPojo() != null) {
            pojoClassLoader = schemaDefinition.getPojo().getClassLoader();
        }
        return new AvroSchema<>(parseSchemaInfo(schemaDefinition, SchemaType.AVRO), pojoClassLoader);
    }

    public static <T> AvroSchema<T> of(Class<T> pojo) {
        return AvroSchema.of(SchemaDefinition.<T>builder().withPojo(pojo).build());
    }

    public static <T> AvroSchema<T> of(Class<T> pojo, Map<String, String> properties) {
        ClassLoader pojoClassLoader = null;
        if (pojo != null) {
            pojoClassLoader = pojo.getClassLoader();
        }
        SchemaDefinition<T> schemaDefinition = SchemaDefinition.<T>builder().withPojo(pojo).withProperties(properties).build();
        return new AvroSchema<>(parseSchemaInfo(schemaDefinition, SchemaType.AVRO), pojoClassLoader);
    }

    @Override
    protected SchemaReader<T> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            log.info("Load schema reader for version({}), schema is : {}, schemaInfo: {}",
                SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                schemaInfo.getSchemaDefinition(), schemaInfo.toString());
            boolean jsr310ConversionEnabled = getJsr310ConversionEnabledFromSchemaInfo(schemaInfo);
            return new AvroReader<>(parseAvroSchema(schemaInfo.getSchemaDefinition()), schema, pojoClassLoader, jsr310ConversionEnabled);
        } else {
            log.warn("No schema found for version({}), use latest schema : {}",
                SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                this.schemaInfo.getSchemaDefinition());
            return reader;
        }
    }

    private static boolean getJsr310ConversionEnabledFromSchemaInfo(SchemaInfo schemaInfo) {
        if (schemaInfo != null) {
            return Boolean.parseBoolean(schemaInfo.getProperties()
                    .getOrDefault(SchemaDefinitionBuilderImpl.JSR310_CONVERSION_ENABLED, "false"));
        }
        return false;
    }

    public static void addLogicalTypeConversions(ReflectData reflectData, boolean jsr310ConversionEnabled) {
        reflectData.addLogicalTypeConversion(new Conversions.DecimalConversion());
        reflectData.addLogicalTypeConversion(new TimeConversions.DateConversion());
        reflectData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        reflectData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        reflectData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        if (jsr310ConversionEnabled) {
            reflectData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        } else {
            try {
                Class.forName("org.joda.time.DateTime");
                reflectData.addLogicalTypeConversion(new JodaTimeConversions.TimestampConversion());
            } catch (ClassNotFoundException e) {
                // Skip if have not provide joda-time dependency.
            }
        }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

}
