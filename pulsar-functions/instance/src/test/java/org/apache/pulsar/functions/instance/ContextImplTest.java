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
package org.apache.pulsar.functions.instance;

import static java.nio.charset.StandardCharsets.UTF_8;
<<<<<<< HEAD
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
=======
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

<<<<<<< HEAD
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.prometheus.client.CollectorRegistry;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;
import org.apache.pulsar.functions.utils.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.slf4j.Logger;
=======
import io.prometheus.client.CollectorRegistry;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Unit test {@link ContextImpl}.
 */
public class ContextImplTest {

    private InstanceConfig config;
    private Logger logger;
    private PulsarClientImpl client;
    private ContextImpl context;
    private Producer producer = mock(Producer.class);

<<<<<<< HEAD
    @Before
=======
    @BeforeMethod
>>>>>>> f773c602c... Test pr 10 (#27)
    public void setup() {
        config = new InstanceConfig();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
            .setUserConfig("")
            .build();
        config.setFunctionDetails(functionDetails);
        logger = mock(Logger.class);
        client = mock(PulsarClientImpl.class);
        when(client.newProducer()).thenReturn(new ProducerBuilderImpl(client, Schema.BYTES));
<<<<<<< HEAD
        when(client.createProducerAsync(Matchers.any(ProducerConfigurationData.class), Matchers.any(Schema.class), eq(null)))
=======
        when(client.createProducerAsync(any(ProducerConfigurationData.class), any(), any()))
>>>>>>> f773c602c... Test pr 10 (#27)
                .thenReturn(CompletableFuture.completedFuture(producer));
        when(client.getSchema(anyString())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(producer.sendAsync(anyString())).thenReturn(CompletableFuture.completedFuture(null));

<<<<<<< HEAD
=======
        TypedMessageBuilder messageBuilder = spy(new TypedMessageBuilderImpl(mock(ProducerBase.class), Schema.STRING));
        doReturn(new CompletableFuture<>()).when(messageBuilder).sendAsync();
        when(producer.newMessage()).thenReturn(messageBuilder);
>>>>>>> f773c602c... Test pr 10 (#27)
        context = new ContextImpl(
            config,
            logger,
            client,
<<<<<<< HEAD
            new ArrayList<>(),
            new EnvironmentBasedSecretsProvider(), new CollectorRegistry(), new String[0],
                Utils.ComponentType.FUNCTION);
    }

    @Test(expected = IllegalStateException.class)
=======
            new EnvironmentBasedSecretsProvider(), new CollectorRegistry(), new String[0],
                FunctionDetails.ComponentType.FUNCTION, null, null);
        context.setCurrentMessageContext((Record<String>) () -> null);
    }

    @Test(expectedExceptions = IllegalStateException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testIncrCounterStateDisabled() {
        context.incrCounter("test-key", 10);
    }

<<<<<<< HEAD
    @Test(expected = IllegalStateException.class)
=======
    @Test(expectedExceptions = IllegalStateException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testGetCounterStateDisabled() {
        context.getCounter("test-key");
    }

<<<<<<< HEAD
    @Test(expected = IllegalStateException.class)
=======
    @Test(expectedExceptions = IllegalStateException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testPutStateStateDisabled() {
        context.putState("test-key", ByteBuffer.wrap("test-value".getBytes(UTF_8)));
    }

<<<<<<< HEAD
    @Test(expected = IllegalStateException.class)
=======
    @Test(expectedExceptions = IllegalStateException.class)
    public void testDeleteStateStateDisabled() {
        context.deleteState("test-key");
    }

    @Test(expectedExceptions = IllegalStateException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testGetStateStateDisabled() {
        context.getState("test-key");
    }

    @Test
    public void testIncrCounterStateEnabled() throws Exception {
<<<<<<< HEAD
        StateContextImpl stateContext = mock(StateContextImpl.class);
        context.setStateContext(stateContext);
        context.incrCounter("test-key", 10L);
        verify(stateContext, times(1)).incr(eq("test-key"), eq(10L));
=======
        context.stateContext = mock(StateContextImpl.class);
        context.incrCounterAsync("test-key", 10L);
        verify(context.stateContext, times(1)).incrCounter(eq("test-key"), eq(10L));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testGetCounterStateEnabled() throws Exception {
<<<<<<< HEAD
        StateContextImpl stateContext = mock(StateContextImpl.class);
        context.setStateContext(stateContext);
        context.getCounter("test-key");
        verify(stateContext, times(1)).getAmount(eq("test-key"));
=======
        context.stateContext = mock(StateContextImpl.class);
        context.getCounterAsync("test-key");
        verify(context.stateContext, times(1)).getCounter(eq("test-key"));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testPutStateStateEnabled() throws Exception {
<<<<<<< HEAD
        StateContextImpl stateContext = mock(StateContextImpl.class);
        context.setStateContext(stateContext);
        ByteBuffer buffer = ByteBuffer.wrap("test-value".getBytes(UTF_8));
        context.putState("test-key", buffer);
        verify(stateContext, times(1)).put(eq("test-key"), same(buffer));
=======
        context.stateContext = mock(StateContextImpl.class);
        ByteBuffer buffer = ByteBuffer.wrap("test-value".getBytes(UTF_8));
        context.putStateAsync("test-key", buffer);
        verify(context.stateContext, times(1)).put(eq("test-key"), same(buffer));
    }

    @Test
    public void testDeleteStateStateEnabled() throws Exception {
        context.stateContext = mock(StateContextImpl.class);
        ByteBuffer buffer = ByteBuffer.wrap("test-value".getBytes(UTF_8));
        context.deleteStateAsync("test-key");
        verify(context.stateContext, times(1)).delete(eq("test-key"));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testGetStateStateEnabled() throws Exception {
<<<<<<< HEAD
        StateContextImpl stateContext = mock(StateContextImpl.class);
        context.setStateContext(stateContext);
        context.getState("test-key");
        verify(stateContext, times(1)).getValue(eq("test-key"));
=======
        context.stateContext = mock(StateContextImpl.class);
        context.getStateAsync("test-key");
        verify(context.stateContext, times(1)).get(eq("test-key"));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testPublishUsingDefaultSchema() throws Exception {
<<<<<<< HEAD
        context.publish("sometopic", "Somevalue");
=======
        context.newOutputMessage("sometopic", null).value("Somevalue").sendAsync();
>>>>>>> f773c602c... Test pr 10 (#27)
    }
 }
