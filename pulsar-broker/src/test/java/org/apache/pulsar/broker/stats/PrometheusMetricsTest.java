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
package org.apache.pulsar.broker.stats;

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class PrometheusMetricsTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMetricsTopicCount() throws Exception{
        String ns1 = "prop/ns-abc1";
        String ns2 = "prop/ns-abc2";
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        String baseTopic1 = "persistent://" + ns1 + "/testMetricsTopicCount";
        String baseTopic2 = "persistent://" + ns2 + "/testMetricsTopicCount";
        for (int i = 0; i < 6; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic1 + UUID.randomUUID().toString());
        }
        for (int i = 0; i < 3; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic2 + UUID.randomUUID().toString());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        Collection<Metric> metric = metrics.get("pulsar_topics_count");
        metric.stream().forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 6.0);
            }
            if (ns2.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 3.0);
            }
        });
    }

    @Test
    public void testPerTopicStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 2 metrics with different tags for each topic
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 3);
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("topic_load_times_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_in_bytes_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_messages_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("subscription"), "test");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("subscription"), "test");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerNamespaceStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 1 metric aggregated per namespace
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 1);
        assertNull(cm.get(0).tags.get("topic"));
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 2);
        assertNull(cm.get(1).tags.get("topic"));
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerConsumerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, true, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 1 metric aggregated per namespace
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 4);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("subscription"), "test");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("consumer_id"), "1");

        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(2).tags.get("subscription"), "test");

        assertEquals(cm.get(3).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(3).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(3).tags.get("subscription"), "test");
        assertEquals(cm.get(3).tags.get("consumer_id"), "0");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 4);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("subscription"), "test");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("consumer_id"), "1");

        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(2).tags.get("subscription"), "test");

        assertEquals(cm.get(3).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(3).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(3).tags.get("subscription"), "test");
        assertEquals(cm.get(3).tags.get("consumer_id"), "0");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    /** Checks for duplicate type definitions for a metric in the Prometheus metrics output. If the Prometheus parser
     finds a TYPE definition for the same metric more than once, it errors out:
     https://github.com/prometheus/prometheus/blob/f04b1b5559a80a4fd1745cf891ce392a056460c9/vendor/github.com/prometheus/common/expfmt/text_parse.go#L499-L502
     This can happen when including topic metrics, since the same metric is reported multiple times with different labels. For example:

     # TYPE pulsar_subscriptions_count gauge
     pulsar_subscriptions_count{cluster="standalone"} 0 1556372982118
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",topic="persistent://public/functions/metadata"} 1.0 1556372982118
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",topic="persistent://public/functions/coordinate"} 1.0 1556372982118
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",topic="persistent://public/functions/assignments"} 1.0 1556372982118

     **/
    // Running the test twice to make sure types are present when generated multiple times
    @Test(invocationCount = 2)
    public void testDuplicateMetricTypeDefinitions() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Map<String, String> typeDefs = new HashMap<String, String>();
        Map<String, String> metricNames = new HashMap<String, String>();

        Pattern typePattern = Pattern.compile("^#\\s+TYPE\\s+(\\w+)\\s+(\\w+)");
        Pattern metricNamePattern = Pattern.compile("^(\\w+)\\{.+");

        Splitter.on("\n").split(metricsStr).forEach(line -> {
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("#")) {
                // Check for duplicate type definitions
                Matcher typeMatcher = typePattern.matcher(line);
                checkArgument(typeMatcher.matches());
                String metricName = typeMatcher.group(1);
                String type = typeMatcher.group(2);

                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "Only one TYPE line may exist for a given metric name."
                if (!typeDefs.containsKey(metricName)) {
                    typeDefs.put(metricName, type);
                } else {
                    fail("Duplicate type definition found for TYPE definition " + metricName);
                    System.out.println(metricsStr);

                }
                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "The TYPE line for a metric name must appear before the first sample is reported for that metric name."
                if (metricNames.containsKey(metricName)) {
                    System.out.println(metricsStr);
                    fail("TYPE definition for " + metricName + " appears after first sample");

                }
            } else {
                Matcher metricMatcher = metricNamePattern.matcher(line);
                checkArgument(metricMatcher.matches());
                String metricName = metricMatcher.group(1);
                metricNames.put(metricName, metricName);
            }
        });

        // Metrics with no type definition
        for (String metricName : metricNames.keySet()) {

            if (!typeDefs.containsKey(metricName)) {
                // This may be OK if this is a _sum or _count metric from a summary
                if(metricName.endsWith("_sum")) {
                    String summaryMetricName = metricName.substring(0, metricName.indexOf("_sum"));
                    if (!typeDefs.containsKey(summaryMetricName)) {
                        fail("Metric " + metricName + " does not have a corresponding summary type definition");
                    }
                } else if (metricName.endsWith("_count")) {
                    String summaryMetricName = metricName.substring(0, metricName.indexOf("_count"));
                    if (!typeDefs.containsKey(summaryMetricName)) {
                        fail("Metric " + metricName + " does not have a corresponding summary type definition");
                    }
                } else {
                    fail("Metric " + metricName + " does not have a type definition");
                }

            }
        }

        p1.close();
        p2.close();
    }

    @Test
    public void testManagedLedgerCacheStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_cache_evictions");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_ml_cache_hits_rate");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        p1.close();
        p2.close();
    }

    @Test
    public void testManagedLedgerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_AddEntryBytesRate");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_ml_AddEntryMessagesRate");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        p1.close();
        p2.close();
    }

    @Test
    public void testManagedLedgerBookieClientStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_scheduler_completed_tasks_0");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_scheduler_queue_0");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_scheduler_total_tasks_0");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_workers_completed_tasks_0");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_workers_task_execution_count");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        p1.close();
        p2.close();
    }

    /**
     * Hacky parsing of Prometheus text format. Sould be good enough for unit tests
     */
    private static Multimap<String, Metric> parseMetrics(String metrics) {
        Multimap<String, Metric> parsed = ArrayListMultimap.create();

        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="sample/standalone/ns1",
        // topic="persistent://sample/standalone/ns1/test-2"} 0.0 1517945780897
        Pattern pattern = Pattern.compile("^(\\w+)\\{([^\\}]+)\\}\\s(-?[\\d\\w\\.-]+)(\\s(\\d+))?$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");

        Splitter.on("\n").split(metrics).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }

            Matcher matcher = pattern.matcher(line);
            assertTrue(matcher.matches());
            String name = matcher.group(1);

            Metric m = new Metric();
            String numericValue = matcher.group(3);
            if (numericValue.equalsIgnoreCase("-Inf")) {
                m.value = Double.NEGATIVE_INFINITY;
            } else if (numericValue.equalsIgnoreCase("+Inf")) {
                m.value = Double.POSITIVE_INFINITY;
            } else {
                m.value = Double.valueOf(numericValue);
            }
            String tags = matcher.group(2);
            Matcher tagsMatcher = tagsPattern.matcher(tags);
            while (tagsMatcher.find()) {
                String tag = tagsMatcher.group(1);
                String value = tagsMatcher.group(2);
                m.tags.put(tag, value);
            }

            parsed.put(name, m);
        });

        return parsed;
    }

    static class Metric {
        Map<String, String> tags = new TreeMap<>();
        double value;

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("tags", tags).add("value", value).toString();
        }
    }

}
