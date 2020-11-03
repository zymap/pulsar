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
package org.apache.pulsar.client.impl.schema.generic;

<<<<<<< HEAD
import static com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
=======
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic avro schema.
 */
<<<<<<< HEAD
class GenericAvroSchema extends GenericSchema {

    private final GenericDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter;
    private BinaryEncoder encoder;
    private final ByteArrayOutputStream byteArrayOutputStream;
    private final GenericDatumReader<org.apache.avro.generic.GenericRecord> datumReader;

    public GenericAvroSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, encoder);
        this.datumWriter = new GenericDatumWriter(schema);
        this.datumReader = new GenericDatumReader(schema);
    }

    @Override
    public synchronized byte[] encode(GenericRecord message) {
        checkArgument(message instanceof GenericAvroRecord);
        GenericAvroRecord gar = (GenericAvroRecord) message;
        try {
            datumWriter.write(gar.getAvroRecord(), this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }

    @Override
    public GenericRecord decode(byte[] bytes) {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            org.apache.avro.generic.GenericRecord avroRecord = datumReader.read(
                null,
                decoder);
            return new GenericAvroRecord(schema, fields, avroRecord);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
=======
@Slf4j
public class GenericAvroSchema extends GenericSchemaImpl {

    public final static String OFFSET_PROP = "__AVRO_READ_OFFSET__";

    public GenericAvroSchema(SchemaInfo schemaInfo) {
        this(schemaInfo, true);
    }

    GenericAvroSchema(SchemaInfo schemaInfo,
                      boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo, useProvidedSchemaAsReaderSchema);
        setReader(new GenericAvroReader(schema));
        setWriter(new GenericAvroWriter(schema));
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new AvroRecordBuilderImpl(this);
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public org.apache.pulsar.client.api.Schema<GenericRecord> clone() {
        org.apache.pulsar.client.api.Schema<GenericRecord> schema =
                GenericAvroSchema.of(schemaInfo, useProvidedSchemaAsReaderSchema);
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        return schema;
    }

    @Override
    protected SchemaReader<GenericRecord> loadReader(BytesSchemaVersion schemaVersion) {
         SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
         if (schemaInfo != null) {
             log.info("Load schema reader for version({}), schema is : {}",
                 SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                 schemaInfo);
             Schema writerSchema = parseAvroSchema(schemaInfo.getSchemaDefinition());
             Schema readerSchema = useProvidedSchemaAsReaderSchema ? schema : writerSchema;
             readerSchema.addProp(OFFSET_PROP, schemaInfo.getProperties().getOrDefault(OFFSET_PROP, "0"));

             return new GenericAvroReader(
                     writerSchema,
                     readerSchema,
                     schemaVersion.get());
         } else {
             log.warn("No schema found for version({}), use latest schema : {}",
                 SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                 this.schemaInfo);
             return reader;
         }
>>>>>>> f773c602c... Test pr 10 (#27)
    }

}
