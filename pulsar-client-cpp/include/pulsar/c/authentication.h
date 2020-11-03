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

<<<<<<< HEAD
=======
#include <pulsar/defines.h>

>>>>>>> f773c602c... Test pr 10 (#27)
#ifdef __cplusplus
extern "C" {
#endif

<<<<<<< HEAD
#pragma GCC visibility push(default)

=======
>>>>>>> f773c602c... Test pr 10 (#27)
typedef struct _pulsar_authentication pulsar_authentication_t;

typedef char *(*token_supplier)(void *);

<<<<<<< HEAD
pulsar_authentication_t *pulsar_authentication_create(const char *dynamicLibPath,
                                                      const char *authParamsString);

pulsar_authentication_t *pulsar_authentication_tls_create(const char *certificatePath,
                                                          const char *privateKeyPath);

pulsar_authentication_t *pulsar_authentication_token_create(const char *token);
pulsar_authentication_t *pulsar_authentication_token_create_with_supplier(token_supplier tokenSupplier,
                                                                          void *ctx);

pulsar_authentication_t *pulsar_authentication_athenz_create(const char *authParamsString);

void pulsar_authentication_free(pulsar_authentication_t *authentication);

#pragma GCC visibility pop
=======
PULSAR_PUBLIC pulsar_authentication_t *pulsar_authentication_create(const char *dynamicLibPath,
                                                                    const char *authParamsString);

PULSAR_PUBLIC pulsar_authentication_t *pulsar_authentication_tls_create(const char *certificatePath,
                                                                        const char *privateKeyPath);

PULSAR_PUBLIC pulsar_authentication_t *pulsar_authentication_token_create(const char *token);
PULSAR_PUBLIC pulsar_authentication_t *pulsar_authentication_token_create_with_supplier(
    token_supplier tokenSupplier, void *ctx);

PULSAR_PUBLIC pulsar_authentication_t *pulsar_authentication_athenz_create(const char *authParamsString);

PULSAR_PUBLIC pulsar_authentication_t *pulsar_authentication_oauth2_create(const char *authParamsString);

PULSAR_PUBLIC void pulsar_authentication_free(pulsar_authentication_t *authentication);
>>>>>>> f773c602c... Test pr 10 (#27)

#ifdef __cplusplus
}
#endif