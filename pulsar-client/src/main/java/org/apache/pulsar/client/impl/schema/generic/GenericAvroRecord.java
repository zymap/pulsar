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

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.Field;
<<<<<<< HEAD
import org.apache.pulsar.client.api.schema.GenericRecord;
=======
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * A generic avro record.
 */
@Slf4j
<<<<<<< HEAD
class GenericAvroRecord implements GenericRecord {

    private final org.apache.avro.Schema schema;
    private final List<Field> fields;
    private final org.apache.avro.generic.GenericRecord record;

    GenericAvroRecord(org.apache.avro.Schema schema,
                      List<Field> fields,
                      org.apache.avro.generic.GenericRecord record) {
        this.schema = schema;
        this.fields = fields;
=======
public class GenericAvroRecord extends VersionedGenericRecord {

    private final org.apache.avro.Schema schema;
    private final org.apache.avro.generic.GenericRecord record;

    public GenericAvroRecord(byte[] schemaVersion,
                      org.apache.avro.Schema schema,
                      List<Field> fields,
                      org.apache.avro.generic.GenericRecord record) {
        super(schemaVersion, fields);
        this.schema = schema;
>>>>>>> f773c602c... Test pr 10 (#27)
        this.record = record;
    }

    @Override
<<<<<<< HEAD
    public List<Field> getFields() {
        return fields;
    }

    @Override
=======
>>>>>>> f773c602c... Test pr 10 (#27)
    public Object getField(String fieldName) {
        Object value = record.get(fieldName);
        if (value instanceof Utf8) {
            return ((Utf8) value).toString();
        } else if (value instanceof org.apache.avro.generic.GenericRecord) {
            org.apache.avro.generic.GenericRecord avroRecord =
                (org.apache.avro.generic.GenericRecord) value;
            org.apache.avro.Schema recordSchema = avroRecord.getSchema();
            List<Field> fields = recordSchema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
<<<<<<< HEAD
            return new GenericAvroRecord(schema, fields, avroRecord);
=======
            return new GenericAvroRecord(schemaVersion, schema, fields, avroRecord);
>>>>>>> f773c602c... Test pr 10 (#27)
        } else {
            return value;
        }
    }

<<<<<<< HEAD
    org.apache.avro.generic.GenericRecord getAvroRecord() {
=======
    public org.apache.avro.generic.GenericRecord getAvroRecord() {
>>>>>>> f773c602c... Test pr 10 (#27)
        return record;
    }

}
