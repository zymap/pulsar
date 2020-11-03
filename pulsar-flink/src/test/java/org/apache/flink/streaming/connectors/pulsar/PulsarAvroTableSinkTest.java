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

<<<<<<< HEAD
=======
import org.apache.commons.lang3.reflect.FieldUtils;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.avro.generated.NasaMission;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor;
<<<<<<< HEAD
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
=======
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarPropertiesExtractor;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Unit test of {@link PulsarAvroTableSink}.
 */
public class PulsarAvroTableSinkTest {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC_NAME = "test_topic";
<<<<<<< HEAD
=======
    private static final Authentication AUTHENTICATION = new AuthenticationDisabled();
>>>>>>> f773c602c... Test pr 10 (#27)
    private static final String ROUTING_KEY = "name";

    private final String[] fieldNames = {"id", "name","start_year","end_year"};
    private final TypeInformation[] typeInformations = {
            TypeInformation.of(Integer.class),
            TypeInformation.of(String.class),
            TypeInformation.of(Integer.class),
            TypeInformation.of(Integer.class)
    };

    /**
     * Test configure PulsarTableSink.
     *
     * @throws Exception
     */
    @Test
    public void testConfigure() throws Exception {
        PulsarAvroTableSink sink = spySink();

        TableSink<Row> configuredSink = sink.configure(fieldNames, typeInformations);

<<<<<<< HEAD
        Assert.assertArrayEquals(fieldNames, configuredSink.getFieldNames());
        Assert.assertArrayEquals(typeInformations, configuredSink.getFieldTypes());
        Assert.assertNotNull(((PulsarAvroTableSink) configuredSink).keyExtractor);
        Assert.assertNotNull(((PulsarAvroTableSink) configuredSink).serializationSchema);
=======
        assertArrayEquals(fieldNames, configuredSink.getFieldNames());
        assertArrayEquals(typeInformations, configuredSink.getFieldTypes());
        assertNotNull(((PulsarAvroTableSink) configuredSink).keyExtractor);
        assertNotNull(((PulsarAvroTableSink) configuredSink).serializationSchema);
>>>>>>> f773c602c... Test pr 10 (#27)
    }


    /**
     * Test emit data stream.
     *
     * @throws Exception
     */
    @Test
    public void testEmitDataStream() throws Exception {
        DataStream mockedDataStream = Mockito.mock(DataStream.class);

        PulsarAvroTableSink sink = spySink();

        sink.emitDataStream(mockedDataStream);

        Mockito.verify(mockedDataStream).addSink(Mockito.any(FlinkPulsarProducer.class));
    }


    private PulsarAvroTableSink spySink() throws Exception {

<<<<<<< HEAD
        PulsarAvroTableSink sink = new PulsarAvroTableSink(SERVICE_URL, TOPIC_NAME, ROUTING_KEY, NasaMission.class);
=======
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(SERVICE_URL);

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(TOPIC_NAME);

        PulsarAvroTableSink sink =
                new PulsarAvroTableSink(clientConf, producerConf, ROUTING_KEY, NasaMission.class);
>>>>>>> f773c602c... Test pr 10 (#27)
        FlinkPulsarProducer producer = Mockito.mock(FlinkPulsarProducer.class);
        PowerMockito.whenNew(
                FlinkPulsarProducer.class
        ).withArguments(
                Mockito.anyString(),
                Mockito.anyString(),
<<<<<<< HEAD
                Mockito.any(SerializationSchema.class),
                Mockito.any(PulsarKeyExtractor.class)
        ).thenReturn(producer);
        Whitebox.setInternalState(sink, "fieldNames", fieldNames);
        Whitebox.setInternalState(sink, "fieldTypes", typeInformations);
        Whitebox.setInternalState(sink, "serializationSchema", Mockito.mock(SerializationSchema.class));
        Whitebox.setInternalState(sink, "keyExtractor", Mockito.mock(PulsarKeyExtractor.class));
=======
                Mockito.any(Authentication.class),
                Mockito.any(SerializationSchema.class),
                Mockito.any(PulsarKeyExtractor.class),
                Mockito.any(PulsarPropertiesExtractor.class)
        ).thenReturn(producer);
        FieldUtils.writeField(sink, "fieldNames", fieldNames, true);
        FieldUtils.writeField(sink, "fieldTypes", typeInformations, true);
        FieldUtils.writeField(sink, "serializationSchema", Mockito.mock(SerializationSchema.class), true);
        FieldUtils.writeField(sink, "keyExtractor", Mockito.mock(PulsarKeyExtractor.class), true);
        FieldUtils.writeField(sink, "propertiesExtractor", Mockito.mock(PulsarPropertiesExtractor.class), true);
>>>>>>> f773c602c... Test pr 10 (#27)
        return sink;
    }

}
