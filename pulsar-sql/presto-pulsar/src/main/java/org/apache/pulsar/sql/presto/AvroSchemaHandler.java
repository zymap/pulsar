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
package org.apache.pulsar.sql.presto;

<<<<<<< HEAD
import io.airlift.log.Logger;

import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.apache.pulsar.shade.io.netty.buffer.ByteBufAllocator;
import org.apache.pulsar.shade.io.netty.util.ReferenceCountUtil;
import org.apache.pulsar.shade.io.netty.util.concurrent.FastThreadLocal;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.List;

public class AvroSchemaHandler implements SchemaHandler {

    private final DatumReader<GenericRecord> datumReader;

    private final List<PulsarColumnHandle> columnHandles;

    private static final FastThreadLocal<BinaryDecoder> decoders =
            new FastThreadLocal<>();

    private static final Logger log = Logger.get(AvroSchemaHandler.class);

    public AvroSchemaHandler(Schema schema, List<PulsarColumnHandle> columnHandles) {
        this.datumReader = new GenericDatumReader<>(schema);
=======
import com.google.common.annotations.VisibleForTesting;

import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;

import java.util.List;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;


/**
 * Schema handler for payload in the Avro format.
 */
public class AvroSchemaHandler implements SchemaHandler {

    private final List<PulsarColumnHandle> columnHandles;

    private final GenericAvroSchema genericAvroSchema;

    private final SchemaInfo schemaInfo;

    private static final Logger log = Logger.get(AvroSchemaHandler.class);

    AvroSchemaHandler(TopicName topicName,
                      PulsarConnectorConfig pulsarConnectorConfig,
                      SchemaInfo schemaInfo,
                      List<PulsarColumnHandle> columnHandles) throws PulsarClientException {
        this(new PulsarSqlSchemaInfoProvider(topicName,
                                pulsarConnectorConfig.getPulsarAdmin()), schemaInfo, columnHandles);
    }

    AvroSchemaHandler(PulsarSqlSchemaInfoProvider pulsarSqlSchemaInfoProvider,
                      SchemaInfo schemaInfo, List<PulsarColumnHandle> columnHandles) {
        this.schemaInfo = schemaInfo;
        this.genericAvroSchema = new GenericAvroSchema(schemaInfo);
        this.genericAvroSchema.setSchemaInfoProvider(pulsarSqlSchemaInfoProvider);
>>>>>>> f773c602c... Test pr 10 (#27)
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(ByteBuf payload) {
<<<<<<< HEAD

        ByteBuf heapBuffer = null;
        try {
            BinaryDecoder decoderFromCache = decoders.get();

            // Make a copy into a heap buffer, since Avro cannot deserialize directly from direct memory
            int size = payload.readableBytes();
            heapBuffer = ByteBufAllocator.DEFAULT.heapBuffer(size, size);
            heapBuffer.writeBytes(payload);

            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(heapBuffer.array(), heapBuffer.arrayOffset(),
                    heapBuffer.readableBytes(), decoderFromCache);
            if (decoderFromCache==null) {
                decoders.set(decoder);
            }
            return this.datumReader.read(null, decoder);
        } catch (IOException e) {
            log.error(e);
        } finally {
            ReferenceCountUtil.safeRelease(heapBuffer);
        }
        return null;
=======
        return genericAvroSchema.decode(payload);
    }

    @Override
    public Object deserialize(ByteBuf payload, byte[] schemaVersion) {
        return genericAvroSchema.decode(payload, schemaVersion);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        try {
<<<<<<< HEAD
            GenericRecord record = (GenericRecord) currentRecord;
            PulsarColumnHandle pulsarColumnHandle = this.columnHandles.get(index);
            Integer[] positionIndices = pulsarColumnHandle.getPositionIndices();
            Object curr = record.get(positionIndices[0]);
            if (curr == null) {
                return null;
            }
            if (positionIndices.length > 0) {
                for (int i = 1 ; i < positionIndices.length; i++) {
                    curr = ((GenericRecord) curr).get(positionIndices[i]);
                    if (curr == null) {
                        return null;
                    }
                }
            }
            return curr;
        } catch (Exception ex) {
            log.debug(ex,"%s", ex);
        }
        return null;
    }
=======
            GenericAvroRecord record = (GenericAvroRecord) currentRecord;
            PulsarColumnHandle pulsarColumnHandle = this.columnHandles.get(index);
            String[] names = pulsarColumnHandle.getFieldNames();

            if (names.length == 1) {
                return record.getField(pulsarColumnHandle.getFieldNames()[0]);
            } else {
                for (int i = 0; i < names.length - 1; i++) {
                    record = (GenericAvroRecord) record.getField(names[i]);
                }
                return record.getField(names[names.length - 1]);
            }
        } catch (Exception ex) {
            log.debug(ex, "%s", ex);
        }
        return null;
    }

    @VisibleForTesting
    GenericAvroSchema getSchema() {
        return this.genericAvroSchema;
    }

    @VisibleForTesting
    SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
