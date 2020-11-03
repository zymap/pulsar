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
package org.apache.pulsar.tests.integration.presto;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

<<<<<<< HEAD
=======
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

>>>>>>> f773c602c... Test pr 10 (#27)
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestBasicPresto extends PulsarTestSuite {

    private static final int NUM_OF_STOCKS = 10;

    @BeforeClass
    public void setupPresto() throws Exception {
<<<<<<< HEAD
        pulsarCluster.startPrestoWorker();
=======
        log.info("[setupPresto]");
        pulsarCluster.startPrestoWorker();
    }

    @AfterClass
    public void teardownPresto() {
        log.info("tearing down...");
        pulsarCluster.stopPrestoWorker();
    }

    @Test
    public void testSimpleSQLQueryBatched() throws Exception {
        testSimpleSQLQuery(true);
    }

    @Test
    public void testSimpleSQLQueryNonBatched() throws Exception {
        testSimpleSQLQuery(false);
    }
    
    public void testSimpleSQLQuery(boolean isBatched) throws Exception {
>>>>>>> f773c602c... Test pr 10 (#27)

        // wait until presto worker started
        ContainerExecResult result;
        do {
            try {
                result = execQuery("show catalogs;");
                assertThat(result.getExitCode()).isEqualTo(0);
                assertThat(result.getStdout()).contains("pulsar", "system");
                break;
            } catch (ContainerExecException cee) {
                if (cee.getResult().getStderr().contains("Presto server is still initializing")) {
                    Thread.sleep(10000);
                } else {
                    throw cee;
                }
            }
        } while (true);
<<<<<<< HEAD
    }

    @AfterClass
    public void teardownPresto() {
        pulsarCluster.stopPrestoWorker();;
    }

    @Test
    public void testSimpleSQLQuery() throws Exception {
=======
>>>>>>> f773c602c... Test pr 10 (#27)

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                                    .build();

<<<<<<< HEAD
        final String stocksTopic = "stocks";
=======
        String stocksTopic;
        if (isBatched) {
            stocksTopic = "stocks_batched";
        } else {
            stocksTopic = "stocks_nonbatched";
        }
>>>>>>> f773c602c... Test pr 10 (#27)

        @Cleanup
        Producer<Stock> producer = pulsarClient.newProducer(JSONSchema.of(Stock.class))
                .topic(stocksTopic)
<<<<<<< HEAD
                .create();


=======
                .enableBatching(isBatched)
                .create();

>>>>>>> f773c602c... Test pr 10 (#27)
        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            final Stock stock = new Stock(i,"STOCK_" + i , 100.0 + i * 10);
            producer.send(stock);
        }
<<<<<<< HEAD

        ContainerExecResult result = execQuery("show schemas in pulsar;");
=======
        producer.flush();

        result = execQuery("show schemas in pulsar;");
>>>>>>> f773c602c... Test pr 10 (#27)
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains("public/default");

        result = execQuery("show tables in pulsar.\"public/default\";");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).contains("stocks");

<<<<<<< HEAD
        ContainerExecResult containerExecResult = execQuery("select * from pulsar.\"public/default\".stocks order by entryid;");
=======
        ContainerExecResult containerExecResult = execQuery(String.format("select * from pulsar.\"public/default\".%s order by entryid;", stocksTopic));
>>>>>>> f773c602c... Test pr 10 (#27)
        assertThat(containerExecResult.getExitCode()).isEqualTo(0);
        log.info("select sql query output \n{}", containerExecResult.getStdout());
        String[] split = containerExecResult.getStdout().split("\n");
        assertThat(split.length).isGreaterThan(NUM_OF_STOCKS - 2);

        String[] split2 = containerExecResult.getStdout().split("\n|,");

        for (int i = 0; i < NUM_OF_STOCKS - 2; ++i) {
            assertThat(split2).contains("\"" + i + "\"");
            assertThat(split2).contains("\"" + "STOCK_" + i + "\"");
            assertThat(split2).contains("\"" + (100.0 + i * 10) + "\"");
        }

<<<<<<< HEAD
=======
        // test predicate pushdown

        String url = String.format("jdbc:presto://%s",  pulsarCluster.getPrestoWorkerContainer().getUrl());
        Connection connection = DriverManager.getConnection(url, "test", null);

        String query = String.format("select * from pulsar" +
                ".\"public/default\".%s order by __publish_time__", stocksTopic);
        log.info("Executing query: {}", query);
        ResultSet res = connection.createStatement().executeQuery(query);

        List<Timestamp> timestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            timestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(timestamps.size()).isGreaterThan(NUM_OF_STOCKS - 2);

        query = String.format("select * from pulsar" +
                ".\"public/default\".%s where __publish_time__ > timestamp '%s' order by __publish_time__", stocksTopic, timestamps.get(timestamps.size() / 2));
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        List<Timestamp> returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(timestamps.size() / 2);

        // Try with a predicate that has a earlier time than any entry
        // Should return all rows
        query = String.format("select * from pulsar" +
                ".\"public/default\".%s where __publish_time__ > from_unixtime(%s) order by __publish_time__", stocksTopic, 0);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(timestamps.size());

        // Try with a predicate that has a latter time than any entry
        // Should return no rows

        query = String.format("select * from pulsar" +
                ".\"public/default\".%s where __publish_time__ > from_unixtime(%s) order by __publish_time__", stocksTopic, 99999999999L);
        log.info("Executing query: {}", query);
        res = connection.createStatement().executeQuery(query);

        returnedTimestamps = new LinkedList<>();
        while (res.next()) {
            printCurrent(res);
            returnedTimestamps.add(res.getTimestamp("__publish_time__"));
        }

        assertThat(returnedTimestamps.size()).isEqualTo(0);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    public static ContainerExecResult execQuery(final String query) throws Exception {
        ContainerExecResult containerExecResult;

        containerExecResult = pulsarCluster.getPrestoWorkerContainer()
                .execCmd("/bin/bash", "-c", PulsarCluster.PULSAR_COMMAND_SCRIPT + " sql --execute " + "'" + query + "'");

        return containerExecResult;

    }

<<<<<<< HEAD
=======
    private static void printCurrent(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) System.out.print(",  ");
            String columnValue = rs.getString(i);
            System.out.print(columnValue + " " + rsmd.getColumnName(i));
        }
        System.out.println("");

    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
