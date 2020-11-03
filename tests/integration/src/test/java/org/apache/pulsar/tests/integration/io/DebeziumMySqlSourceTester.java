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
package org.apache.pulsar.tests.integration.io;

<<<<<<< HEAD
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.tests.integration.containers.DebeziumMySQLContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;
=======
import java.io.Closeable;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.DebeziumMySQLContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * A tester for testing Debezium MySQL source.
 *
 * It reads binlog from MySQL, and store the debezium output into Pulsar.
 * This test verify that the target topic contains wanted number messages.
 *
 * Debezium MySQL Container is "debezium/example-mysql:0.8",
 * which is a MySQL database server preconfigured with an inventory database.
 */
@Slf4j
<<<<<<< HEAD
public class DebeziumMySqlSourceTester extends SourceTester<DebeziumMySQLContainer> {

    private static final String NAME = "kafka-connect-adaptor";
=======
public class DebeziumMySqlSourceTester extends SourceTester<DebeziumMySQLContainer> implements Closeable {

    private static final String NAME = "debezium-mysql";
>>>>>>> f773c602c... Test pr 10 (#27)

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumMySQLContainer debeziumMySqlContainer;

    private final PulsarCluster pulsarCluster;

<<<<<<< HEAD
    public DebeziumMySqlSourceTester(PulsarCluster cluster) {
=======
    public DebeziumMySqlSourceTester(PulsarCluster cluster, String converterClassName) {
>>>>>>> f773c602c... Test pr 10 (#27)
        super(NAME);
        this.pulsarCluster = cluster;
        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

<<<<<<< HEAD
        sourceConfig.put("task.class", "io.debezium.connector.mysql.MySqlConnectorTask");
        sourceConfig.put("database.hostname", "mysql");
=======
        sourceConfig.put("database.hostname", DebeziumMySQLContainer.NAME);
>>>>>>> f773c602c... Test pr 10 (#27)
        sourceConfig.put("database.port", "3306");
        sourceConfig.put("database.user", "debezium");
        sourceConfig.put("database.password", "dbz");
        sourceConfig.put("database.server.id", "184054");
        sourceConfig.put("database.server.name", "dbserver1");
        sourceConfig.put("database.whitelist", "inventory");
<<<<<<< HEAD
        sourceConfig.put("database.history", "org.apache.pulsar.io.debezium.PulsarDatabaseHistory");
        sourceConfig.put("database.history.pulsar.topic", "history-topic");
        sourceConfig.put("database.history.pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        sourceConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        sourceConfig.put("pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("offset.storage.topic", "offset-topic");
=======
        sourceConfig.put("pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("key.converter", converterClassName);
        sourceConfig.put("value.converter", converterClassName);
        sourceConfig.put("topic.namespace", "debezium/mysql-" +
                (converterClassName.endsWith("AvroConverter") ? "avro" : "json"));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public void setServiceContainer(DebeziumMySQLContainer container) {
        log.info("start debezium mysql server container.");
        debeziumMySqlContainer = container;
<<<<<<< HEAD
        pulsarCluster.startService("mysql", debeziumMySqlContainer);
=======
        pulsarCluster.startService(DebeziumMySQLContainer.NAME, debeziumMySqlContainer);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public void prepareSource() throws Exception {
        log.info("debezium mysql server already contains preconfigured data.");
    }

    @Override
<<<<<<< HEAD
=======
    public void prepareInsertEvent() throws Exception {
        this.debeziumMySqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u root -pdebezium -e 'SELECT * FROM inventory.products'");
        this.debeziumMySqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u root -pdebezium " +
                        "-e \"INSERT INTO inventory.products(name, description, weight) " +
                        "values('test-debezium', 'This is description', 2.0)\"");
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        this.debeziumMySqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u root -pdebezium " +
                        "-e \"UPDATE inventory.products set description='update description', weight=10 " +
                        "WHERE name='test-debezium'\"");
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        this.debeziumMySqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u root -pdebezium -e 'SELECT * FROM inventory.products'");
        this.debeziumMySqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u root -pdebezium " +
                        "-e \"DELETE FROM inventory.products WHERE name='test-debezium'\"");
        this.debeziumMySqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u root -pdebezium -e 'SELECT * FROM inventory.products'");
    }


    @Override
>>>>>>> f773c602c... Test pr 10 (#27)
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        log.info("debezium mysql server already contains preconfigured data.");
        return null;
    }

<<<<<<< HEAD
    public void validateSourceResult(Consumer<String> consumer, Map<String, String> kvs) throws Exception {
        int recordsNumber = 0;
        Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
        while(msg != null) {
            recordsNumber ++;
            log.info("Received message: {}.", msg.getValue());
            Assert.assertTrue(msg.getValue().contains("dbserver1.inventory.products"));
            consumer.acknowledge(msg);
            msg = consumer.receive(1, TimeUnit.SECONDS);
        }

        Assert.assertEquals(recordsNumber, 9);
        log.info("Stop debezium mysql server container. topic: {} has {} records.", consumer.getTopic(), recordsNumber);
    }
=======
    @Override
    public void close() {
        if (pulsarCluster != null) {
            pulsarCluster.stopService(DebeziumMySQLContainer.NAME, debeziumMySqlContainer);
        }
    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
