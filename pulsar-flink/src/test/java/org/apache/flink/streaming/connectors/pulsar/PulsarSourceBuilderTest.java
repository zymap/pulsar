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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
=======
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.regex.Pattern;

/**
 * Tests for PulsarSourceBuilder
 */
public class PulsarSourceBuilderTest {

    private PulsarSourceBuilder pulsarSourceBuilder;

<<<<<<< HEAD
    @Before
=======
    @BeforeMethod
>>>>>>> f773c602c... Test pr 10 (#27)
    public void before() {
        pulsarSourceBuilder = PulsarSourceBuilder.builder(new TestDeserializationSchema());
    }

    @Test
<<<<<<< HEAD
    public void testBuild() {
=======
    public void testBuild() throws PulsarClientException {
>>>>>>> f773c602c... Test pr 10 (#27)
        SourceFunction sourceFunction = pulsarSourceBuilder
                .serviceUrl("testServiceUrl")
                .topic("testTopic")
                .subscriptionName("testSubscriptionName")
<<<<<<< HEAD
=======
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
>>>>>>> f773c602c... Test pr 10 (#27)
                .build();
        Assert.assertNotNull(sourceFunction);
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithoutSettingRequiredProperties() {
        pulsarSourceBuilder.build();
    }

    @Test(expected = IllegalArgumentException.class)
=======

    @Test
    public void testBuildWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName("testSubscriptionName");
        consumerConf.setSubscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

        SourceFunction sourceFunction = pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
        Assert.assertNotNull(sourceFunction);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBuildWithoutSettingRequiredProperties() throws PulsarClientException {
        pulsarSourceBuilder.build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testServiceUrlWithNull() {
        pulsarSourceBuilder.serviceUrl(null);
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testServiceUrlWithBlank() {
        pulsarSourceBuilder.serviceUrl(" ");
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testTopicWithNull() {
        pulsarSourceBuilder.topic(null);
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testTopicWithBlank() {
        pulsarSourceBuilder.topic(" ");
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testTopicsWithNull() {
        pulsarSourceBuilder.topics(null);
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testTopicsWithBlank() {
        pulsarSourceBuilder.topics(Arrays.asList(" ", " "));
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testTopicPatternWithNull() {
        pulsarSourceBuilder.topicsPattern(null);
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testTopicPatternAlreadySet() {
        pulsarSourceBuilder.topicsPattern(Pattern.compile("persistent://tenants/ns/topic-*"));
        pulsarSourceBuilder.topicsPattern(Pattern.compile("persistent://tenants/ns/topic-my-*"));
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testTopicPattenStringWithNull() {
        pulsarSourceBuilder.topicsPatternString(null);
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testSubscriptionNameWithNull() {
        pulsarSourceBuilder.subscriptionName(null);
    }

<<<<<<< HEAD
    @Test(expected = IllegalArgumentException.class)
=======
    @Test(expectedExceptions = IllegalArgumentException.class)
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testSubscriptionNameWithBlank() {
        pulsarSourceBuilder.subscriptionName(" ");
    }

<<<<<<< HEAD
=======
    @Test(expectedExceptions = NullPointerException.class)
    public void testSubscriptionInitialPosition() {
        pulsarSourceBuilder.subscriptionInitialPosition(null);
    }

>>>>>>> f773c602c... Test pr 10 (#27)
    private class TestDeserializationSchema<T> implements DeserializationSchema<T> {

        @Override
        public T deserialize(byte[] bytes) throws IOException {
            return null;
        }

        @Override
        public boolean isEndOfStream(T t) {
            return false;
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return null;
        }
    }
<<<<<<< HEAD
=======

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testServiceUrlNullWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(null);

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testServiceUrl")));
        consumerConf.setSubscriptionName("testSubscriptionName");

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testServiceUrlWithBlankWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(StringUtils.EMPTY);

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName("testSubscriptionName");

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTopicPatternWithNullWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");
        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicsPattern(null);
        consumerConf.setSubscriptionName("testSubscriptionName");

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSubscriptionNameWithNullWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName(null);

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSubscriptionNameWithBlankWithConfPojo() throws PulsarClientException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(new HashSet<String>(Arrays.asList("testTopic")));
        consumerConf.setSubscriptionName(StringUtils.EMPTY);

        pulsarSourceBuilder
                .pulsarAllClientConf(clientConf)
                .pulsarAllConsumerConf(consumerConf)
                .build();
    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
