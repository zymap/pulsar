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
package org.apache.pulsar.client.examples;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TokenRefresh {
    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        String token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJjbGllbnQifQ.CmgQAkhL46u8G6OzT1iXrEOrVotL9z63gZdnLF8_JALPN7kP2G8kGkdm3Ho8DeeKPjYd_aaWB9Fo2Lgv7HCIRN6VVB6go6kWI25OHz_M2xWBtzHjYWOqjY6g56K5PItXwKNA4ft5AFf5iZxX2YikxuKwq1MytqFqN53aev1MPkSvQyZtThh6OlS3oZwEVRxT8-akks1DcUjWTu5KAu87gHG8CGYvITOLd8R9BOW2J1ZUEOpJUKQaY7oZBx0-egLFYGJJb4NLoSzDFfKXg9lascTW7owbV9pJ6xi3MyOkkDdTlzaQzAVlU1MnOek7yBwTbfqcBkMVb-QKdgjDgXaHdA";

        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .authentication(new AuthenticationToken(() -> {
                System.out.println(new Date().toString());
                System.out.println(token);
                return token;

            })).build();
//        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").authentication(new AuthenticationToken(token)).build();

        Producer<String> producer = client.newProducer(Schema.STRING).topic("test-refresh-topic").create();
        int i = 0;
        while (true) {
            producer.send("message" + i);
            System.out.println("Send message ..." + i);
            TimeUnit.SECONDS.sleep(1);
            i++;
        }
    }
}
