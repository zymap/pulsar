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
package org.apache.flink.batch.connectors.pulsar.serialization;

<<<<<<< HEAD
=======
import java.io.ByteArrayOutputStream;
import java.io.IOException;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

<<<<<<< HEAD
import java.io.ByteArrayOutputStream;
import java.io.IOException;

=======
>>>>>>> f773c602c... Test pr 10 (#27)
/**
 * Avro Serialization Schema to serialize Dataset records to Avro.
 */
public class AvroSerializationSchema<T extends SpecificRecord> implements SerializationSchema<T> {

    private static final long serialVersionUID = -6691140169413760919L;

    @Override
    public byte[] serialize(T t) {
        if (null == t) {
            return null;
        }

        // Writer to serialize Avro record into a byte array.
        DatumWriter<T> writer = new SpecificDatumWriter<>(t.getSchema());
        // Output stream to serialize records into byte array.
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        // Low-level class for serialization of Avro values.
        Encoder encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
        arrayOutputStream.reset();
        try {
<<<<<<< HEAD
            writer.write(t,encoder);
=======
            writer.write(t, encoder);
>>>>>>> f773c602c... Test pr 10 (#27)
            encoder.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error while serializing the record to Avro", e);
        }

        return arrayOutputStream.toByteArray();
    }

}
