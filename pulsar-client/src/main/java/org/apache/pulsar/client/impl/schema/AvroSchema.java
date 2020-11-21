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

import org.apache.avro.Conversions;
import org.apache.avro.data.JodaTimeConversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.schema.reader.MultiVersionAvroReader;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.pulsar.client.impl.schema.util.SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo;
import static org.apache.pulsar.client.impl.schema.util.SchemaUtil.parseSchemaInfo;

/**
 * An AVRO schema implementation.
 */
@Slf4j
public class AvroSchema<T> extends AvroBaseStructSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchema.class);

    private ClassLoader pojoClassLoader;

    private AvroSchema(SchemaInfo schemaInfo, ClassLoader pojoClassLoader) {
        super(schemaInfo);
        this.pojoClassLoader = pojoClassLoader;
        boolean jsr310ConversionEnabled = getJsr310ConversionEnabledFromSchemaInfo(schemaInfo);
        setReader(new MultiVersionAvroReader<>(schema, pojoClassLoader, getJsr310ConversionEnabledFromSchemaInfo(schemaInfo)));
        setWriter(new AvroWriter<>(schema, jsr310ConversionEnabled));
    }

    private AvroSchema(SchemaReader<T> reader, SchemaWriter<T> writer, SchemaInfo schemaInfo) {
        super(schemaInfo);
        setReader(reader);
        setWriter(writer);
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
        if (schemaDefinition.getSchemaReaderOpt().isPresent() && schemaDefinition.getSchemaWriterOpt().isPresent()) {
            return new AvroSchema<>(schemaDefinition.getSchemaReaderOpt().get(),
                    schemaDefinition.getSchemaWriterOpt().get(), parseSchemaInfo(schemaDefinition, SchemaType.AVRO));
        }
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
    }

}
