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
package org.apache.pulsar.io.datagenerator;

import io.codearte.jfairy.Fairy;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import java.util.Map;
import java.util.Optional;

<<<<<<< HEAD

public class DataGeneratorSource implements Source<Person> {

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {

=======
public class DataGeneratorSource implements Source<Person> {

    private Fairy fairy;
    private DataGeneratorSourceConfig dataGeneratorSourceConfig;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        dataGeneratorSourceConfig = DataGeneratorSourceConfig.loadOrGetDefault(config);
        this.fairy = Fairy.create();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public Record<Person> read() throws Exception {
<<<<<<< HEAD
        Thread.sleep(50);
=======
        Thread.sleep(dataGeneratorSourceConfig.getSleepBetweenMessages());
>>>>>>> f773c602c... Test pr 10 (#27)
        return new Record<Person>() {
            @Override
            public Optional<String> getKey() {
                return Optional.empty();
            }

            @Override
            public Person getValue() {
<<<<<<< HEAD
                return new Person(Fairy.create().person());
=======
                return new Person(fairy.person());
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        };
    }

    @Override
    public void close() throws Exception {

    }
}
