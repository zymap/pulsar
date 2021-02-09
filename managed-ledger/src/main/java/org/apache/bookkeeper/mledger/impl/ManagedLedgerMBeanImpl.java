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
package org.apache.bookkeeper.mledger.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import io.netty.util.concurrent.FastThreadLocal;
import io.prometheus.client.Collector;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.SummaryMetricFamily;
import lombok.Getter;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

public class ManagedLedgerMBeanImpl implements ManagedLedgerMXBean {

    public static final long[] ENTRY_LATENCY_BUCKETS_USEC = {500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
        200_000, 1000_000};
    public static final long[] ENTRY_SIZE_BUCKETS_BYTES = {128, 512, 1024, 2048, 4096, 16_384, 102_400, 1_232_896};

    private final ManagedLedgerImpl managedLedger;

    private final Rate addEntryOps = new Rate();
    private final Rate addEntryOpsFailed = new Rate();
    private final Rate readEntriesOps = new Rate();
    private final Rate readEntriesOpsFailed = new Rate();
    private final Rate markDeleteOps = new Rate();

    private final LongAdder dataLedgerOpenOp = new LongAdder();
    private final LongAdder dataLedgerCloseOp = new LongAdder();
    private final LongAdder dataLedgerCreateOp = new LongAdder();
    private final LongAdder dataLedgerDeleteOp = new LongAdder();
    private final LongAdder cursorLedgerOpenOp = new LongAdder();
    private final LongAdder cursorLedgerCloseOp = new LongAdder();
    private final LongAdder cursorLedgerCreateOp = new LongAdder();
    private final LongAdder cursorLedgerDeleteOp = new LongAdder();

    // addEntryLatencyStatsUsec measure total latency including time entry spent while waiting in queue 
    private final StatsBuckets addEntryLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    // ledgerAddEntryLatencyStatsUsec measure latency to persist entry into ledger
    private final StatsBuckets ledgerAddEntryLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    private final StatsBuckets ledgerSwitchLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    private final StatsBuckets entryStats = new StatsBuckets(ENTRY_SIZE_BUCKETS_BYTES);

    // readEntryLatencyStatsUses measure total latency from sending read request to the request complete
    @Getter
    private final StatsBuckets readEntryLatencyStatsUses = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    private static final Summary READ_ENTRY_LATENCY = Summary.build("ml_read_entry_latency", "-")
        .quantile(0.0)
        .quantile(0.50)
        .quantile(0.95)
        .quantile(0.99)
        .quantile(0.999)
        .quantile(0.9999)
        .quantile(1.0)
        .register();

    public ManagedLedgerMBeanImpl(ManagedLedgerImpl managedLedger) {
        this.managedLedger = managedLedger;
    }

    public void refreshStats(long period, TimeUnit unit) {
        double seconds = unit.toMillis(period) / 1000.0;
        addEntryOps.calculateRate(seconds);
        addEntryOpsFailed.calculateRate(seconds);
        readEntriesOps.calculateRate(seconds);
        readEntriesOpsFailed.calculateRate(seconds);
        markDeleteOps.calculateRate(seconds);

        addEntryLatencyStatsUsec.refresh();
        ledgerAddEntryLatencyStatsUsec.refresh();
        ledgerSwitchLatencyStatsUsec.refresh();
        entryStats.refresh();
        readEntryLatencyStatsUses.refresh();
        Summary.rotateLatencyCollection();
    }

    public void addAddEntrySample(long size) {
        addEntryOps.recordEvent(size);
        entryStats.addValue(size);
    }

    public void addMarkDeleteOp() {
        markDeleteOps.recordEvent();
    }

    public void recordAddEntryError() {
        addEntryOpsFailed.recordEvent();
    }

    public void recordReadEntriesError() {
        readEntriesOpsFailed.recordEvent();
    }

    public void addAddEntryLatencySample(long latency, TimeUnit unit) {
        addEntryLatencyStatsUsec.addValue(unit.toMicros(latency));
    }

    public void addReadEntryLatencySample(long latency, TimeUnit unit) {
        readEntryLatencyStatsUses.addValue(unit.toMicros(latency));
        READ_ENTRY_LATENCY.observe(latency, unit);
    }

    public void addLedgerAddEntryLatencySample(long latency, TimeUnit unit) {
        ledgerAddEntryLatencyStatsUsec.addValue(unit.toMicros(latency));
    }

    public void addLedgerSwitchLatencySample(long latency, TimeUnit unit) {
        ledgerSwitchLatencyStatsUsec.addValue(unit.toMicros(latency));
    }

    public void addReadEntriesSample(int count, long totalSize) {
        readEntriesOps.recordMultipleEvents(count, totalSize);
    }

    public void startDataLedgerOpenOp() {
        dataLedgerOpenOp.increment();
    }

    public void endDataLedgerOpenOp() {
        dataLedgerOpenOp.decrement();
    }

    public void startDataLedgerCloseOp() {
        dataLedgerCloseOp.increment();
    }

    public void endDataLedgerCloseOp() {
        dataLedgerCloseOp.decrement();
    }

    public void startDataLedgerCreateOp() {
        dataLedgerCreateOp.increment();
    }

    public void endDataLedgerCreateOp() {
        dataLedgerCreateOp.decrement();
    }

    public void startDataLedgerDeleteOp() {
        dataLedgerDeleteOp.increment();
    }

    public void endDataLedgerDeleteOp() {
        dataLedgerDeleteOp.decrement();
    }

    public void startCursorLedgerOpenOp() {
        cursorLedgerOpenOp.increment();
    }

    public void endCursorLedgerOpenOp() {
        cursorLedgerOpenOp.decrement();
    }

    public void startCursorLedgerCloseOp() {
        cursorLedgerCloseOp.increment();
    }

    public void endCursorLedgerCloseOp() {
        cursorLedgerCloseOp.decrement();
    }

    public void startCursorLedgerCreateOp() {
        cursorLedgerCreateOp.increment();
    }

    public void endCursorLedgerCreateOp() {
        cursorLedgerCreateOp.decrement();
    }

    public void startCursorLedgerDeleteOp() {
        cursorLedgerDeleteOp.increment();
    }

    public void endCursorLedgerDeleteOp() {
        cursorLedgerDeleteOp.decrement();
    }

    @Override
    public String getName() {
        return managedLedger.getName();
    }

    @Override
    public double getAddEntryMessagesRate() {
        return addEntryOps.getRate();
    }

    @Override
    public double getAddEntryBytesRate() {
        return addEntryOps.getValueRate();
    }

    @Override
    public double getReadEntriesRate() {
        return readEntriesOps.getRate();
    }

    @Override
    public double getReadEntriesBytesRate() {
        return readEntriesOps.getValueRate();
    }

    @Override
    public long getAddEntrySucceed() {
        return addEntryOps.getCount();
    }

    @Override
    public long getAddEntryErrors() {
        return addEntryOpsFailed.getCount();
    }

    @Override
    public long getReadEntriesSucceeded() {
        return readEntriesOps.getCount();
    }

    @Override
    public long getReadEntriesErrors() {
        return readEntriesOpsFailed.getCount();
    }

    @Override
    public double getMarkDeleteRate() {
        return markDeleteOps.getRate();
    }

    @Override
    public double getEntrySizeAverage() {
        return entryStats.getAvg();
    }

    @Override
    public long[] getEntrySizeBuckets() {
        return entryStats.getBuckets();
    }

    @Override
    public double getAddEntryLatencyAverageUsec() {
        return addEntryLatencyStatsUsec.getAvg();
    }

    @Override
    public long[] getAddEntryLatencyBuckets() {
        return addEntryLatencyStatsUsec.getBuckets();
    }

    @Override
    public double getLedgerAddEntryLatencyAverageUsec() {
        return ledgerAddEntryLatencyStatsUsec.getAvg();
    }

    @Override
    public long[] getLedgerAddEntryLatencyBuckets() {
        return ledgerAddEntryLatencyStatsUsec.getBuckets();
    }

    @Override
    public long[] getLedgerSwitchLatencyBuckets() {
        return ledgerSwitchLatencyStatsUsec.getBuckets();
    }

    @Override
    public StatsBuckets getInternalAddEntryLatencyBuckets() {
        return addEntryLatencyStatsUsec;
    }

    @Override
    public StatsBuckets getInternalLedgerAddEntryLatencyBuckets() {
        return ledgerAddEntryLatencyStatsUsec;
    }

    @Override
    public double getReadEntryLatency() {
        return readEntryLatencyStatsUses.getAvg();
    }

    @Override
    public long[] getReadEntryLatencyBuckets() {
        return readEntryLatencyStatsUses.getBuckets();
    }

    @Override
    public StatsBuckets getInternalEntrySizeBuckets() {
        return entryStats;
    }

    @Override
    public double getLedgerSwitchLatencyAverageUsec() {
        return ledgerSwitchLatencyStatsUsec.getAvg();
    }

    @Override
    public long getStoredMessagesSize() {
        return managedLedger.getTotalSize() * managedLedger.getConfig().getWriteQuorumSize();
    }

    @Override
    public long getNumberOfMessagesInBacklog() {
        long count = 0;

        for (ManagedCursor cursor : managedLedger.getCursors()) {
            count += cursor.getNumberOfEntriesInBacklog(false);
        }

        return count;
    }

    @Override
    public PendingBookieOpsStats getPendingBookieOpsStats() {
        PendingBookieOpsStats result = new PendingBookieOpsStats();
        result.dataLedgerOpenOp = dataLedgerOpenOp.longValue();
        result.dataLedgerCloseOp = dataLedgerCloseOp.longValue();
        result.dataLedgerCreateOp = dataLedgerCreateOp.longValue();
        result.dataLedgerDeleteOp = dataLedgerDeleteOp.longValue();
        result.cursorLedgerOpenOp = cursorLedgerOpenOp.longValue();
        result.cursorLedgerCloseOp = cursorLedgerCloseOp.longValue();
        result.cursorLedgerCreateOp = cursorLedgerCreateOp.longValue();
        result.cursorLedgerDeleteOp = cursorLedgerDeleteOp.longValue();
        return result;
    }

    static class Summary extends SimpleCollector<Summary.Child> implements Collector.Describable {

        private static final List<DataSketchesSummaryLogger> LOGGERS = new ArrayList<>();

        public static class Builder extends SimpleCollector.Builder<Builder, Summary> {
            private final List<Double> quantiles = new ArrayList<>();

            public Builder quantile(double quantile) {
                this.quantiles.add(quantile);
                return this;
            }

            @Override
            public Summary create() {
                return new Summary(this);
            }
        }

        static class Child {
            private final DataSketchesSummaryLogger logger;
            private final List<Double> quantiles;

            public Child(List<Double> quantiles) {
                this.quantiles = quantiles;
                this.logger = new DataSketchesSummaryLogger();
                synchronized (LOGGERS) {
                    if (!quantiles.isEmpty()) {
                        LOGGERS.add(logger);
                    }
                }
            }

            public void observe(long eventLatency, TimeUnit unit) {
                logger.registerEvent(eventLatency, unit);
            }
        }

        public static Builder build(String name, String help) {
            return build().name(name).help(help);
        }

        public static Builder build() {
            return new Builder();
        }

        private final List<Double> quantiles;

        private Summary(Builder builder) {
            super(builder);
            this.quantiles = builder.quantiles;
            this.clear();
        }

        @Override
        protected Child newChild() {
            if (quantiles != null) {
                return new Child(quantiles);
            } else {
                return new Child(Collections.emptyList());
            }
        }

        public void observe(long eventLatency, TimeUnit unit) {
            noLabelsChild.observe(eventLatency, unit);
        }

        public static void rotateLatencyCollection() {
            synchronized (LOGGERS) {
                for (int i = 0, n = LOGGERS.size(); i < n; i++) {
                    LOGGERS.get(i).rotateLatencyCollection();
                }
            }
        }

        @Override
        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>();
            for (Map.Entry<List<String>, Child> c : children.entrySet()) {
                Child child = c.getValue();
                List<String> labelNamesWithQuantile = new ArrayList<String>(labelNames);
                labelNamesWithQuantile.add("quantile");
                for (Double q : child.quantiles) {
                    List<String> labelValuesWithQuantile = new ArrayList<String>(c.getKey());
                    labelValuesWithQuantile.add(doubleToGoString(q));
                    samples.add(new MetricFamilySamples.Sample(fullname, labelNamesWithQuantile, labelValuesWithQuantile,
                        child.logger.getQuantileValue(q)));
                }
                samples.add(new MetricFamilySamples.Sample(fullname + "_count", labelNames, c.getKey(),
                    child.logger.getCount()));
                samples.add(
                    new MetricFamilySamples.Sample(fullname + "_sum", labelNames, c.getKey(), child.logger.getSum()));
            }

            return familySamplesList(Collector.Type.SUMMARY, samples);
        }

        @Override
        public List<MetricFamilySamples> describe() {
            return Collections.singletonList(new SummaryMetricFamily(fullname, help, labelNames));
        }
    }

    static class DataSketchesSummaryLogger {

        /*
         * Use 2 rotating thread local accessor so that we can safely swap them.
         */
        private volatile ThreadLocalAccessor current;
        private volatile ThreadLocalAccessor replacement;

        /*
         * These are the sketches where all the aggregated results are published.
         */
        private volatile DoublesSketch values;
        private final LongAdder countAdder = new LongAdder();
        private final LongAdder sumAdder = new LongAdder();

        DataSketchesSummaryLogger() {
            this.current = new ThreadLocalAccessor();
            this.replacement = new ThreadLocalAccessor();
        }

        public void registerEvent(long eventLatency, TimeUnit unit) {
            double valueMillis = unit.toMicros(eventLatency) / 1000.0;

            countAdder.increment();
            sumAdder.add((long) valueMillis);

            LocalData localData = current.localData.get();

            long stamp = localData.lock.readLock();
            try {
                localData.successSketch.update(valueMillis);
            } finally {
                localData.lock.unlockRead(stamp);
            }
        }

        public void rotateLatencyCollection() {
            // Swap current with replacement
            ThreadLocalAccessor local = current;
            current = replacement;
            replacement = local;

            final DoublesUnion aggregateValues = new DoublesUnionBuilder().build();
            local.map.forEach((localData, b) -> {
                long stamp = localData.lock.writeLock();
                try {
                    aggregateValues.update(localData.successSketch);
                    localData.successSketch.reset();
                } finally {
                    localData.lock.unlockWrite(stamp);
                }
            });

            values = aggregateValues.getResultAndReset();
        }

        public long getCount() {
            return countAdder.sum();
        }

        public long getSum() {
            return sumAdder.sum();
        }

        public double getQuantileValue(double quantile) {
            DoublesSketch s = values;
            return s != null ? s.getQuantile(quantile) : Double.NaN;
        }

        private static class LocalData {
            private final DoublesSketch successSketch = new DoublesSketchBuilder().build();
            private final StampedLock lock = new StampedLock();
        }

        private static class ThreadLocalAccessor {
            private final Map<LocalData, Boolean> map = new ConcurrentHashMap<>();
            private final FastThreadLocal<LocalData> localData = new FastThreadLocal<LocalData>() {

                @Override
                protected LocalData initialValue() throws Exception {
                    LocalData localData = new LocalData();
                    map.put(localData, Boolean.TRUE);
                    return localData;
                }

                @Override
                protected void onRemoval(LocalData value) throws Exception {
                    map.remove(value);
                }
            };
        }
    }
}
