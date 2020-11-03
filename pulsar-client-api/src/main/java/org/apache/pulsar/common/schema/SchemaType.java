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

/**
<<<<<<< HEAD
 * Types of supported schema for Pulsar messages
 */
public enum SchemaType {
    /**
     * No schema defined
     */
    NONE,

    /**
     * boolean schema defined
     * @since 2.3.0
     */
    BOOLEAN,

    /**
     * Simple String encoding with UTF-8
     */
    STRING,
=======
 * Types of supported schema for Pulsar messages.
 *
 * <p>Ideally we should have just one single set of enum definitions
 * for schema type. but we have 3 locations of defining schema types.
 *
 * <p>when you are adding a new schema type that whose
 * schema info is required to be recorded in schema registry,
 * add corresponding schema type into `pulsar-common/src/main/proto/PulsarApi.proto`
 * and `pulsar-broker/src/main/proto/SchemaRegistryFormat.proto`.
 */
public enum SchemaType {
    /**
     * No schema defined.
     */
    NONE(0),

    /**
     * Simple String encoding with UTF-8.
     */
    STRING(1),

    /**
     * JSON object encoding and validation.
     */
    JSON(2),

    /**
     * Protobuf message encoding and decoding.
     */
    PROTOBUF(3),

    /**
     * Serialize and deserialize via avro.
     */
    AVRO(4),

    /**
     * boolean schema defined.
     * @since 2.3.0
     */
    BOOLEAN(5),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * A 8-byte integer.
     */
<<<<<<< HEAD
    INT8,
=======
    INT8(6),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * A 16-byte integer.
     */
<<<<<<< HEAD
    INT16,
=======
    INT16(7),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * A 32-byte integer.
     */
<<<<<<< HEAD
    INT32,
=======
    INT32(8),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * A 64-byte integer.
     */
<<<<<<< HEAD
    INT64,
=======
    INT64(9),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * A float number.
     */
<<<<<<< HEAD
    FLOAT,

    /**
     * A double number
     */
    DOUBLE,

    /**
     * A bytes array.
     */
    BYTES,

    /**
     * JSON object encoding and validation
     */
    JSON,

    /**
     * Protobuf message encoding and decoding
     */
    PROTOBUF,

    /**
     * Serialize and deserialize via avro
     */
    AVRO,
=======
    FLOAT(10),

    /**
     * A double number.
     */
    DOUBLE(11),

    /**
     * Date.
     * @since 2.4.0
     */
    DATE(12),

    /**
     * Time.
     * @since 2.4.0
     */
    TIME(13),

    /**
     * Timestamp.
     * @since 2.4.0
     */
    TIMESTAMP(14),

    /**
     * A Schema that contains Key Schema and Value Schema.
     */
    KEY_VALUE(15),

    /**
     * Instant.
     */
    INSTANT(16),

    /**
     * LocalDate.
     */
    LOCAL_DATE(17),

    /**
     * LocalTime.
     */
    LOCAL_TIME(18),

    /**
     * LocalDateTime.
     */
    LOCAL_DATE_TIME(19),

    //
    // Schemas that don't have schema info. the value should be negative.
    //

    /**
     * A bytes array.
     */
    BYTES(-1),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * Auto Detect Schema Type.
     */
    @Deprecated
<<<<<<< HEAD
    AUTO,
=======
    AUTO(-2),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * Auto Consume Type.
     */
<<<<<<< HEAD
    AUTO_CONSUME,
=======
    AUTO_CONSUME(-3),
>>>>>>> f773c602c... Test pr 10 (#27)

    /**
     * Auto Publish Type.
     */
<<<<<<< HEAD
    AUTO_PUBLISH,

    /**
     * A Schema that contains Key Schema and Value Schema.
     */
    KEY_VALUE
=======
    AUTO_PUBLISH(-4);

    int value;

    SchemaType(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static SchemaType valueOf(int value) {
        switch (value) {
          case 0: return NONE;
          case 1: return STRING;
          case 2: return JSON;
          case 3: return PROTOBUF;
          case 4: return AVRO;
          case 5: return BOOLEAN;
          case 6: return INT8;
          case 7: return INT16;
          case 8: return INT32;
          case 9: return INT64;
          case 10: return FLOAT;
          case 11: return DOUBLE;
          case 12: return DATE;
          case 13: return TIME;
          case 14: return TIMESTAMP;
          case 15: return KEY_VALUE;
          case 16: return INSTANT;
          case 17: return LOCAL_DATE;
          case 18: return LOCAL_TIME;
          case 19: return LOCAL_DATE_TIME;
          case -1: return BYTES;
          case -2: return AUTO;
          case -3: return AUTO_CONSUME;
          case -4: return AUTO_PUBLISH;
          default: return NONE;
        }
      }


    public boolean isPrimitive() {
        return isPrimitiveType(this);
    }

    public boolean isStruct() {
        return isStructType(this);
    }

    public static boolean isPrimitiveType(SchemaType type) {
        switch (type) {
            case STRING:
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case BYTES:
            case INSTANT:
            case LOCAL_DATE:
            case LOCAL_TIME:
            case LOCAL_DATE_TIME:
            case NONE:
                return true;
            default:
                return false;
        }

    }

    public static boolean isStructType(SchemaType type) {
        switch (type) {
            case AVRO:
            case JSON:
            case PROTOBUF:
                return true;
            default:
                return false;
        }
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
