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
typedef enum { pulsar_DEBUG = 0, pulsar_INFO = 1, pulsar_WARN = 2, pulsar_ERROR = 3 } pulsar_logger_level_t;

typedef void (*pulsar_logger)(pulsar_logger_level_t level, const char *file, int line, const char *message,
                              void *ctx);

typedef struct _pulsar_client_configuration pulsar_client_configuration_t;
typedef struct _pulsar_authentication pulsar_authentication_t;

<<<<<<< HEAD
pulsar_client_configuration_t *pulsar_client_configuration_create();

void pulsar_client_configuration_free(pulsar_client_configuration_t *conf);
=======
PULSAR_PUBLIC pulsar_client_configuration_t *pulsar_client_configuration_create();

PULSAR_PUBLIC void pulsar_client_configuration_free(pulsar_client_configuration_t *conf);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Set the authentication method to be used with the broker
 *
 * @param authentication the authentication data to use
 */
<<<<<<< HEAD
void pulsar_client_configuration_set_auth(pulsar_client_configuration_t *conf,
                                          pulsar_authentication_t *authentication);
=======
PULSAR_PUBLIC void pulsar_client_configuration_set_auth(pulsar_client_configuration_t *conf,
                                                        pulsar_authentication_t *authentication);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Set timeout on client operations (subscribe, create producer, close, unsubscribe)
 * Default is 30 seconds.
 *
 * @param timeout the timeout after which the operation will be considered as failed
 */
<<<<<<< HEAD
void pulsar_client_configuration_set_operation_timeout_seconds(pulsar_client_configuration_t *conf,
                                                               int timeout);
=======
PULSAR_PUBLIC void pulsar_client_configuration_set_operation_timeout_seconds(
    pulsar_client_configuration_t *conf, int timeout);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * @return the client operations timeout in seconds
 */
<<<<<<< HEAD
int pulsar_client_configuration_get_operation_timeout_seconds(pulsar_client_configuration_t *conf);
=======
PULSAR_PUBLIC int pulsar_client_configuration_get_operation_timeout_seconds(
    pulsar_client_configuration_t *conf);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Set the number of IO threads to be used by the Pulsar client. Default is 1
 * thread.
 *
 * @param threads number of threads
 */
<<<<<<< HEAD
void pulsar_client_configuration_set_io_threads(pulsar_client_configuration_t *conf, int threads);
=======
PULSAR_PUBLIC void pulsar_client_configuration_set_io_threads(pulsar_client_configuration_t *conf,
                                                              int threads);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * @return the number of IO threads to use
 */
<<<<<<< HEAD
int pulsar_client_configuration_get_io_threads(pulsar_client_configuration_t *conf);
=======
PULSAR_PUBLIC int pulsar_client_configuration_get_io_threads(pulsar_client_configuration_t *conf);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Set the number of threads to be used by the Pulsar client when delivering messages
 * through message listener. Default is 1 thread per Pulsar client.
 *
 * If using more than 1 thread, messages for distinct MessageListener will be
 * delivered in different threads, however a single MessageListener will always
 * be assigned to the same thread.
 *
 * @param threads number of threads
 */
<<<<<<< HEAD
void pulsar_client_configuration_set_message_listener_threads(pulsar_client_configuration_t *conf,
                                                              int threads);
=======
PULSAR_PUBLIC void pulsar_client_configuration_set_message_listener_threads(
    pulsar_client_configuration_t *conf, int threads);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * @return the number of IO threads to use
 */
<<<<<<< HEAD
int pulsar_client_configuration_get_message_listener_threads(pulsar_client_configuration_t *conf);
=======
PULSAR_PUBLIC int pulsar_client_configuration_get_message_listener_threads(
    pulsar_client_configuration_t *conf);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Number of concurrent lookup-requests allowed on each broker-connection to prevent overload on broker.
 * <i>(default: 50000)</i> It should be configured with higher value only in case of it requires to
 * produce/subscribe on
 * thousands of topic using created {@link PulsarClient}
 *
 * @param concurrentLookupRequest
 */
<<<<<<< HEAD
void pulsar_client_configuration_set_concurrent_lookup_request(pulsar_client_configuration_t *conf,
                                                               int concurrentLookupRequest);
=======
PULSAR_PUBLIC void pulsar_client_configuration_set_concurrent_lookup_request(
    pulsar_client_configuration_t *conf, int concurrentLookupRequest);
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * @return Get configured total allowed concurrent lookup-request.
 */
<<<<<<< HEAD
int pulsar_client_configuration_get_concurrent_lookup_request(pulsar_client_configuration_t *conf);

void pulsar_client_configuration_set_logger(pulsar_client_configuration_t *conf, pulsar_logger logger,
                                            void *ctx);

void pulsar_client_configuration_set_use_tls(pulsar_client_configuration_t *conf, int useTls);

int pulsar_client_configuration_is_use_tls(pulsar_client_configuration_t *conf);

void pulsar_client_configuration_set_tls_trust_certs_file_path(pulsar_client_configuration_t *conf,
                                                               const char *tlsTrustCertsFilePath);

const char *pulsar_client_configuration_get_tls_trust_certs_file_path(pulsar_client_configuration_t *conf);

void pulsar_client_configuration_set_tls_allow_insecure_connection(pulsar_client_configuration_t *conf,
                                                                   int allowInsecure);

int pulsar_client_configuration_is_tls_allow_insecure_connection(pulsar_client_configuration_t *conf);
=======
PULSAR_PUBLIC int pulsar_client_configuration_get_concurrent_lookup_request(
    pulsar_client_configuration_t *conf);

PULSAR_PUBLIC void pulsar_client_configuration_set_logger(pulsar_client_configuration_t *conf,
                                                          pulsar_logger logger, void *ctx);

PULSAR_PUBLIC void pulsar_client_configuration_set_use_tls(pulsar_client_configuration_t *conf, int useTls);

PULSAR_PUBLIC int pulsar_client_configuration_is_use_tls(pulsar_client_configuration_t *conf);

PULSAR_PUBLIC void pulsar_client_configuration_set_tls_trust_certs_file_path(
    pulsar_client_configuration_t *conf, const char *tlsTrustCertsFilePath);

PULSAR_PUBLIC const char *pulsar_client_configuration_get_tls_trust_certs_file_path(
    pulsar_client_configuration_t *conf);

PULSAR_PUBLIC void pulsar_client_configuration_set_tls_allow_insecure_connection(
    pulsar_client_configuration_t *conf, int allowInsecure);

PULSAR_PUBLIC int pulsar_client_configuration_is_tls_allow_insecure_connection(
    pulsar_client_configuration_t *conf);
>>>>>>> f773c602c... Test pr 10 (#27)

/*
 * Initialize stats interval in seconds. Stats are printed and reset after every 'statsIntervalInSeconds'.
 * Set to 0 in order to disable stats collection.
 */
<<<<<<< HEAD
void pulsar_client_configuration_set_stats_interval_in_seconds(pulsar_client_configuration_t *conf,
                                                               const unsigned int interval);

int pulsar_client_configuration_is_validate_hostname(pulsar_client_configuration_t *conf);

void pulsar_client_configuration_set_validate_hostname(pulsar_client_configuration_t *conf,
                                                       int validateHostName);
=======
PULSAR_PUBLIC void pulsar_client_configuration_set_stats_interval_in_seconds(
    pulsar_client_configuration_t *conf, const unsigned int interval);

PULSAR_PUBLIC int pulsar_client_configuration_is_validate_hostname(pulsar_client_configuration_t *conf);

PULSAR_PUBLIC void pulsar_client_configuration_set_validate_hostname(pulsar_client_configuration_t *conf,
                                                                     int validateHostName);
>>>>>>> f773c602c... Test pr 10 (#27)

/*
 * Get the stats interval set in the client.
 */
<<<<<<< HEAD
const unsigned int pulsar_client_configuration_get_stats_interval_in_seconds(
    pulsar_client_configuration_t *conf);

#pragma GCC visibility pop

=======
PULSAR_PUBLIC const unsigned int pulsar_client_configuration_get_stats_interval_in_seconds(
    pulsar_client_configuration_t *conf);

>>>>>>> f773c602c... Test pr 10 (#27)
#ifdef __cplusplus
}
#endif
