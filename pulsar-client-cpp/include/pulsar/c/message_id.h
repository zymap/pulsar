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

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
<<<<<<< HEAD

#pragma GCC visibility push(default)
=======
#include <pulsar/defines.h>
>>>>>>> f773c602c... Test pr 10 (#27)

typedef struct _pulsar_message_id pulsar_message_id_t;

/**
 * MessageId representing the "earliest" or "oldest available" message stored in the topic
 */
<<<<<<< HEAD
const pulsar_message_id_t *pulsar_message_id_earliest();
=======
PULSAR_PUBLIC const pulsar_message_id_t *pulsar_message_id_earliest();
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * MessageId representing the "latest" or "last published" message in the topic
 */
<<<<<<< HEAD
const pulsar_message_id_t *pulsar_message_id_latest();
=======
PULSAR_PUBLIC const pulsar_message_id_t *pulsar_message_id_latest();
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Serialize the message id into a binary string for storing
 */
<<<<<<< HEAD
void *pulsar_message_id_serialize(pulsar_message_id_t *messageId, int *len);
=======
PULSAR_PUBLIC void *pulsar_message_id_serialize(pulsar_message_id_t *messageId, int *len);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Deserialize a message id from a binary string
 */
<<<<<<< HEAD
pulsar_message_id_t *pulsar_message_id_deserialize(const void *buffer, uint32_t len);

char *pulsar_message_id_str(pulsar_message_id_t *messageId);

void pulsar_message_id_free(pulsar_message_id_t *messageId);

#pragma GCC visibility pop
=======
PULSAR_PUBLIC pulsar_message_id_t *pulsar_message_id_deserialize(const void *buffer, uint32_t len);

PULSAR_PUBLIC char *pulsar_message_id_str(pulsar_message_id_t *messageId);

PULSAR_PUBLIC void pulsar_message_id_free(pulsar_message_id_t *messageId);
>>>>>>> f773c602c... Test pr 10 (#27)

#ifdef __cplusplus
}
#endif