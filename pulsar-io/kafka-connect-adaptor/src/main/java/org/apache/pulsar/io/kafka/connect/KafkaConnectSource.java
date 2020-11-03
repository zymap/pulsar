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
package org.apache.pulsar.io.kafka.connect;

<<<<<<< HEAD
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
=======
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaWrappedSchema;
import org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroData;

import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * A pulsar source that runs
 */
@Slf4j
<<<<<<< HEAD
public class KafkaConnectSource implements Source<KeyValue<byte[], byte[]>> {

    // kafka connect related variables
    private SourceTaskContext sourceTaskContext;
    @Getter
    private SourceTask sourceTask;
    private Converter keyConverter;
    private Converter valueConverter;

    // pulsar io related variables
    private Iterator<SourceRecord> currentBatch = null;
    private CompletableFuture<Void> flushFuture;
    private OffsetBackingStore offsetStore;
    private OffsetStorageReader offsetReader;
    @Getter
    private OffsetStorageWriter offsetWriter;
    // number of outstandingRecords that have been polled but not been acked
    private AtomicInteger outstandingRecords = new AtomicInteger(0);

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        Map<String, String> stringConfig = new HashMap<>();
        config.forEach((key, value) -> {
            if (value instanceof String) {
                stringConfig.put(key, (String) value);
            }
        });

        // get the source class name from config and create source task from reflection
        sourceTask = ((Class<? extends SourceTask>)Class.forName(stringConfig.get(TaskConfig.TASK_CLASS_CONFIG)))
            .asSubclass(SourceTask.class)
            .getDeclaredConstructor()
            .newInstance();

        // initialize the key and value converter
        keyConverter = ((Class<? extends Converter>)Class.forName(stringConfig.get(PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG)))
            .asSubclass(Converter.class)
            .getDeclaredConstructor()
            .newInstance();
        valueConverter = ((Class<? extends Converter>)Class.forName(stringConfig.get(PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG)))
            .asSubclass(Converter.class)
            .getDeclaredConstructor()
            .newInstance();

        keyConverter.configure(config, true);
        valueConverter.configure(config, false);

        offsetStore = new PulsarOffsetBackingStore();
        offsetStore.configure(new PulsarKafkaWorkerConfig(stringConfig));
        offsetStore.start();

        offsetReader = new OffsetStorageReaderImpl(
            offsetStore,
            "pulsar-kafka-connect-adaptor",
            keyConverter,
            valueConverter
        );
        offsetWriter = new OffsetStorageWriter(
            offsetStore,
            "pulsar-kafka-connect-adaptor",
            keyConverter,
            valueConverter
        );

        sourceTaskContext = new PulsarIOSourceTaskContext(offsetReader);

        sourceTask.initialize(sourceTaskContext);
        sourceTask.start(stringConfig);
    }

    @Override
    public synchronized Record<KeyValue<byte[], byte[]>> read() throws Exception {
        while (true) {
            if (currentBatch == null) {
                flushFuture = new CompletableFuture<>();
                List<SourceRecord> recordList = sourceTask.poll();
                if (recordList == null || recordList.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                outstandingRecords.addAndGet(recordList.size());
                currentBatch = recordList.iterator();
            }
            if (currentBatch.hasNext()) {
                return processSourceRecord(currentBatch.next());
            } else {
                // there is no records any more, then waiting for the batch to complete writing
                // to sink and the offsets are committed as well, then do next round read.
                flushFuture.get();
                flushFuture = null;
                currentBatch = null;
            }
        }
    }

    @Override
    public void close() {
        sourceTask.stop();
    }

    private synchronized Record<KeyValue<byte[], byte[]>> processSourceRecord(final SourceRecord srcRecord) {
=======
public class KafkaConnectSource extends AbstractKafkaConnectSource<KeyValue<byte[], byte[]>> {

    private final Cache<org.apache.kafka.connect.data.Schema, KafkaSchemaWrappedSchema> readerCache =
            CacheBuilder.newBuilder().maximumSize(10000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build();

    private boolean jsonWithEnvelope = false;
    static private final String JSON_WITH_ENVELOPE_CONFIG = "json-with-envelope";

    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        if (config.get(JSON_WITH_ENVELOPE_CONFIG) != null) {
            jsonWithEnvelope = Boolean.parseBoolean(config.get(JSON_WITH_ENVELOPE_CONFIG).toString());
            config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, jsonWithEnvelope);
        } else {
            config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        }
        log.info("jsonWithEnvelope: {}", jsonWithEnvelope);
        super.open(config, sourceContext);
    }


    public synchronized KafkaSourceRecord processSourceRecord(final SourceRecord srcRecord) {
>>>>>>> f773c602c... Test pr 10 (#27)
        KafkaSourceRecord record = new KafkaSourceRecord(srcRecord);
        offsetWriter.offset(srcRecord.sourcePartition(), srcRecord.sourceOffset());
        return record;
    }

    private static Map<String, String> PROPERTIES = Collections.emptyMap();
    private static Optional<Long> RECORD_SEQUENCE = Optional.empty();
    private static long FLUSH_TIMEOUT_MS = 2000;

<<<<<<< HEAD
    private class KafkaSourceRecord implements Record<KeyValue<byte[], byte[]>>  {
        @Getter
        Optional<String> key;
        @Getter
        KeyValue<byte[], byte[]> value;
        @Getter
        Optional<String> topicName;
        @Getter
        Optional<Long> eventTime;
        @Getter
        Optional<String> partitionId;
        @Getter
        Optional<String> destinationTopic;

        KafkaSourceRecord(SourceRecord srcRecord) {
            byte[] keyBytes = keyConverter.fromConnectData(
                srcRecord.topic(), srcRecord.keySchema(), srcRecord.key());
            byte[] valueBytes = valueConverter.fromConnectData(
                srcRecord.topic(), srcRecord.valueSchema(), srcRecord.value());
            if (keyBytes != null) {
                this.key = Optional.of(Base64.getEncoder().encodeToString(keyBytes));
            }
            this.value = new KeyValue(keyBytes, valueBytes);

            this.topicName = Optional.of(srcRecord.topic());
=======
    private class KafkaSourceRecord extends AbstractKafkaSourceRecord<KeyValue<byte[], byte[]>> implements KVRecord<byte[], byte[]> {

        KafkaSourceRecord(SourceRecord srcRecord) {
            super(srcRecord);
            AvroData avroData = new AvroData(1000);
            byte[] keyBytes = keyConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.keySchema(), srcRecord.key());
            this.key = keyBytes != null ? Optional.of(Base64.getEncoder().encodeToString(keyBytes)) : Optional.empty();

            byte[] valueBytes = valueConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.valueSchema(), srcRecord.value());

            this.value = new KeyValue<>(keyBytes, valueBytes);

            this.topicName = Optional.of(srcRecord.topic());

            if (srcRecord.keySchema() != null) {
                keySchema = readerCache.getIfPresent(srcRecord.keySchema());
            }
            if (srcRecord.valueSchema() != null) {
                valueSchema = readerCache.getIfPresent(srcRecord.valueSchema());
            }

            if (srcRecord.keySchema() != null && keySchema == null) {
                keySchema = new KafkaSchemaWrappedSchema(
                        avroData.fromConnectSchema(srcRecord.keySchema()), keyConverter);
                readerCache.put(srcRecord.keySchema(), keySchema);
            }

            if (srcRecord.valueSchema() != null && valueSchema == null) {
                valueSchema = new KafkaSchemaWrappedSchema(
                        avroData.fromConnectSchema(srcRecord.valueSchema()), valueConverter);
                readerCache.put(srcRecord.valueSchema(), valueSchema);
            }

>>>>>>> f773c602c... Test pr 10 (#27)
            this.eventTime = Optional.ofNullable(srcRecord.timestamp());
            this.partitionId = Optional.of(srcRecord.sourcePartition()
                .entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(",")));
<<<<<<< HEAD
            this.destinationTopic = Optional.of(srcRecord.topic());
        }

        @Override
        public Optional<Long> getRecordSequence() {
            return RECORD_SEQUENCE;
        }

        @Override
        public Map<String, String> getProperties() {
            return PROPERTIES;
        }

        private void completedFlushOffset(Throwable error, Void result) {
            if (error != null) {
                log.error("Failed to flush offsets to storage: ", error);
                currentBatch = null;
                offsetWriter.cancelFlush();
                flushFuture.completeExceptionally(new Exception("No Offsets Added Error"));
            } else {
                log.trace("Finished flushing offsets to storage");
                currentBatch = null;
                flushFuture.complete(null);
=======
        }

        @Override
        public boolean isEmpty(){
            return this.value.getValue() == null;
        }

        @Override
        public Schema<byte[]> getKeySchema() {
            if (jsonWithEnvelope || keySchema == null) {
                return Schema.BYTES;
            } else {
                return keySchema;
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }

        @Override
<<<<<<< HEAD
        public void ack() {
            // TODO: should flush for each batch. not wait for a time for acked all.
            // How to handle order between each batch. QueueList<pair<batch, automicInt>>. check if head is all acked.
            boolean canFlush = (outstandingRecords.decrementAndGet() == 0);

            // consumed all the records, flush the offsets
            if (canFlush && flushFuture != null) {
                if (!offsetWriter.beginFlush()) {
                    log.error("When beginFlush, No offsets to commit!");
                    flushFuture.completeExceptionally(new Exception("No Offsets Added Error when beginFlush"));
                    return;
                }

                Future<Void> doFlush = offsetWriter.doFlush(this::completedFlushOffset);
                if (doFlush == null) {
                    // Offsets added in processSourceRecord, But here no offsets to commit
                    log.error("No offsets to commit!");
                    flushFuture.completeExceptionally(new Exception("No Offsets Added Error"));
                    return;
                }

                // Wait until the offsets are flushed
                try {
                    doFlush.get(FLUSH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    sourceTask.commit();
                } catch (InterruptedException e) {
                    log.warn("Flush of {} offsets interrupted, cancelling", this);
                    offsetWriter.cancelFlush();
                } catch (ExecutionException e) {
                    log.error("Flush of {} offsets threw an unexpected exception: ", this, e);
                    offsetWriter.cancelFlush();
                } catch (TimeoutException e) {
                    log.error("Timed out waiting to flush {} offsets to storage", this);
                    offsetWriter.cancelFlush();
                }
=======
        public Schema<byte[]> getValueSchema() {
            if (jsonWithEnvelope || valueSchema == null) {
                return Schema.BYTES;
            } else {
                return valueSchema;
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }

        @Override
<<<<<<< HEAD
        public void fail() {
            if (flushFuture != null) {
                flushFuture.completeExceptionally(new Exception("Sink Error"));
            }
        }
    }
=======
        public KeyValueEncodingType getKeyValueEncodingType() {
            if (jsonWithEnvelope) {
                return KeyValueEncodingType.INLINE;
            } else {
                return KeyValueEncodingType.SEPARATED;
            }
        }

    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
