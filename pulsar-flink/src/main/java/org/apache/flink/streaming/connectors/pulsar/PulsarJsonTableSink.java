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
package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;
<<<<<<< HEAD
=======
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Base class for {@link PulsarTableSink} that serializes data in JSON format.
 */
public class PulsarJsonTableSink extends PulsarTableSink {

    /**
     * Create PulsarJsonTableSink.
     *
     * @param serviceUrl          pulsar service url
     * @param topic               topic in pulsar to which table is written
<<<<<<< HEAD
     * @param producerConf        producer configuration
=======
     * @param authentication      authetication info required by pulsar client
>>>>>>> f773c602c... Test pr 10 (#27)
     * @param routingKeyFieldName routing key field name
     */
    public PulsarJsonTableSink(
            String serviceUrl,
            String topic,
<<<<<<< HEAD
            String routingKeyFieldName) {
        super(serviceUrl, topic, routingKeyFieldName);
=======
            Authentication authentication,
            String routingKeyFieldName) {
        super(serviceUrl, topic, authentication, routingKeyFieldName);
    }

    public PulsarJsonTableSink(
            ClientConfigurationData clientConfigurationData,
            ProducerConfigurationData producerConfigurationData,
            String routingKeyFieldName) {
        super(clientConfigurationData, producerConfigurationData, routingKeyFieldName);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    protected SerializationSchema<Row> createSerializationSchema(RowTypeInfo rowSchema) {
        return new JsonRowSerializationSchema(rowSchema);
    }

    @Override
    protected PulsarTableSink createSink() {
        return new PulsarJsonTableSink(
<<<<<<< HEAD
                serviceUrl,
                topic,
=======
                clientConfigurationData,
                producerConfigurationData,
>>>>>>> f773c602c... Test pr 10 (#27)
                routingKeyFieldName);
    }
}
