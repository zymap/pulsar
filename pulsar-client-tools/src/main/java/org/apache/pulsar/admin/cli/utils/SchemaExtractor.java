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
package org.apache.pulsar.admin.cli.utils;

import org.apache.pulsar.client.api.Schema;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.schema.SchemaDefinition;
>>>>>>> f773c602c... Test pr 10 (#27)

import java.nio.charset.StandardCharsets;

public class SchemaExtractor {

<<<<<<< HEAD
    public static String getJsonSchemaInfo(Class clazz) {

        return new String(Schema.JSON(clazz).getSchemaInfo().getSchema(), StandardCharsets.UTF_8);
    }

    public static String getAvroSchemaInfo(Class clazz) {

        return new String(Schema.AVRO(clazz).getSchemaInfo().getSchema(), StandardCharsets.UTF_8);
=======
    public static String getJsonSchemaInfo(SchemaDefinition schemaDefinition) {

        return new String(Schema.JSON(schemaDefinition).getSchemaInfo().getSchema(),
                          StandardCharsets.UTF_8);
    }

    public static String getAvroSchemaInfo(SchemaDefinition schemaDefinition) {

        return new String(Schema.AVRO(schemaDefinition).getSchemaInfo().getSchema(),
                          StandardCharsets.UTF_8);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

}
