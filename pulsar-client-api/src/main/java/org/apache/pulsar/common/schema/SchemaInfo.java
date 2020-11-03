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
package org.apache.pulsar.common.schema;

<<<<<<< HEAD
=======
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.Collections;
import java.util.Map;

import lombok.AllArgsConstructor;
<<<<<<< HEAD
=======
import lombok.Builder;
>>>>>>> f773c602c... Test pr 10 (#27)
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
<<<<<<< HEAD

=======
import org.apache.pulsar.client.internal.DefaultImplementation;

/**
 * Information about the schema.
 */
>>>>>>> f773c602c... Test pr 10 (#27)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
<<<<<<< HEAD
=======
@Builder
>>>>>>> f773c602c... Test pr 10 (#27)
public class SchemaInfo {

    @EqualsAndHashCode.Exclude
    private String name;

    /**
<<<<<<< HEAD
     * The schema data in AVRO JSON format
=======
     * The schema data in AVRO JSON format.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    private byte[] schema;

    /**
<<<<<<< HEAD
     * The type of schema (AVRO, JSON, PROTOBUF, etc..)
=======
     * The type of schema (AVRO, JSON, PROTOBUF, etc..).
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    private SchemaType type;

    /**
<<<<<<< HEAD
     * Additional properties of the schema definition (implementation defined)
     */
    private Map<String, String> properties = Collections.emptyMap();
=======
     * Additional properties of the schema definition (implementation defined).
     */
    private Map<String, String> properties = Collections.emptyMap();

    public String getSchemaDefinition() {
        if (null == schema) {
            return "";
        }

        switch (type) {
            case AVRO:
            case JSON:
            case PROTOBUF:
                return new String(schema, UTF_8);
            case KEY_VALUE:
                KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
                    DefaultImplementation.decodeKeyValueSchemaInfo(this);
                return DefaultImplementation.jsonifyKeyValueSchemaInfo(schemaInfoKeyValue);
            default:
                return Base64.getEncoder().encodeToString(schema);
        }
    }

    @Override
    public String toString(){
        return DefaultImplementation.jsonifySchemaInfo(this);
    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
