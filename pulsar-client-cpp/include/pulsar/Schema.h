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
#pragma once

#include <map>

#include <iosfwd>
#include <memory>
<<<<<<< HEAD

#pragma GCC visibility push(default)
=======
#include <string>
#include <pulsar/defines.h>
>>>>>>> f773c602c... Test pr 10 (#27)

namespace pulsar {

enum SchemaType
{
    /**
     * No schema defined
     */
    NONE = 0,

    /**
     * Simple String encoding with UTF-8
     */
    STRING = 1,

    /**
<<<<<<< HEAD
     * A 8-byte integer.
     */
    INT8 = 2,

    /**
     * A 16-byte integer.
     */
    INT16 = 3,

    /**
     * A 32-byte integer.
     */
    INT32 = 4,

    /**
     * A 64-byte integer.
     */
    INT64 = 5,

    /**
     * A float number.
     */
    FLOAT = 6,

    /**
     * A double number
     */
    DOUBLE = 7,

    /**
     * A bytes array.
     */
    BYTES = 8,

    /**
     * JSON object encoding and validation
     */
    JSON = 9,

    /**
     * Protobuf message encoding and decoding
     */
    PROTOBUF = 10,

    /**
     * Serialize and deserialize via Avro
     */
    AVRO = 11,

    /**
     * Auto Consume Type.
     */
    AUTO_CONSUME = 13,

    /**
     * Auto Publish Type.
     */
    AUTO_PUBLISH = 14,

    /**
     * A Schema that contains Key Schema and Value Schema.
     */
    KEY_VALUE = 15,
};

// Return string representation of result code
const char *strSchemaType(SchemaType schemaType);
=======
     * JSON object encoding and validation
     */
    JSON = 2,

    /**
     * Protobuf message encoding and decoding
     */
    PROTOBUF = 3,

    /**
     * Serialize and deserialize via Avro
     */
    AVRO = 4,

    /**
     * A 8-byte integer.
     */
    INT8 = 6,

    /**
     * A 16-byte integer.
     */
    INT16 = 7,

    /**
     * A 32-byte integer.
     */
    INT32 = 8,

    /**
     * A 64-byte integer.
     */
    INT64 = 9,

    /**
     * A float number.
     */
    FLOAT = 10,

    /**
     * A double number
     */
    DOUBLE = 11,

    /**
     * A Schema that contains Key Schema and Value Schema.
     */
    KEY_VALUE = 15,

    /**
     * A bytes array.
     */
    BYTES = -1,

    /**
     * Auto Consume Type.
     */
    AUTO_CONSUME = -3,

    /**
     * Auto Publish Type.
     */
    AUTO_PUBLISH = -4,
};

// Return string representation of result code
PULSAR_PUBLIC const char *strSchemaType(SchemaType schemaType);
>>>>>>> f773c602c... Test pr 10 (#27)

class SchemaInfoImpl;

typedef std::map<std::string, std::string> StringMap;

/**
 * Encapsulates data around the schema definition
 */
<<<<<<< HEAD
class SchemaInfo {
=======
class PULSAR_PUBLIC SchemaInfo {
>>>>>>> f773c602c... Test pr 10 (#27)
   public:
    SchemaInfo();

    /**
     * @param schemaType the schema type
     * @param name the name of the schema definition
     * @param schema the schema definition as a JSON string
     * @param properties a map of custom defined properties attached to the schema
     */
    SchemaInfo(SchemaType schemaType, const std::string &name, const std::string &schema,
               const StringMap &properties = StringMap());

    /**
     * @return the schema type
     */
    SchemaType getSchemaType() const;

    /**
     * @return the name of the schema definition
     */
    const std::string &getName() const;

    /**
     * @return the schema definition as a JSON string
     */
    const std::string &getSchema() const;

    /**
     * @return a map of custom defined properties attached to the schema
     */
    const StringMap &getProperties() const;

   private:
    typedef std::shared_ptr<SchemaInfoImpl> SchemaInfoImplPtr;
    SchemaInfoImplPtr impl_;
};

}  // namespace pulsar

<<<<<<< HEAD
std::ostream &operator<<(std::ostream &s, pulsar::SchemaType schemaType);

#pragma GCC visibility pop
=======
PULSAR_PUBLIC std::ostream &operator<<(std::ostream &s, pulsar::SchemaType schemaType);
>>>>>>> f773c602c... Test pr 10 (#27)
