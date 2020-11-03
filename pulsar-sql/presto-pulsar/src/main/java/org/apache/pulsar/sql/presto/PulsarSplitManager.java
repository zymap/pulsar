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
package org.apache.pulsar.sql.presto;

<<<<<<< HEAD
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
=======
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.QUERY_REJECTED;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries;
import static org.apache.pulsar.sql.presto.PulsarConnectorUtils.restoreNamespaceDelimiterIfNeeded;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.inject.Inject;
>>>>>>> f773c602c... Test pr 10 (#27)
import lombok.Data;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
<<<<<<< HEAD
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
<<<<<<< HEAD
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.com.google.common.base.Predicate;
import org.apache.pulsar.shade.org.apache.bookkeeper.conf.ClientConfiguration;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries;

=======
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * The class helping to manage splits.
 */
>>>>>>> f773c602c... Test pr 10 (#27)
public class PulsarSplitManager implements ConnectorSplitManager {

    private final String connectorId;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final PulsarAdmin pulsarAdmin;

    private static final Logger log = Logger.get(PulsarSplitManager.class);

<<<<<<< HEAD
=======
    private ObjectMapper objectMapper = new ObjectMapper();

>>>>>>> f773c602c... Test pr 10 (#27)
    @Inject
    public PulsarSplitManager(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
<<<<<<< HEAD
                                          SplitSchedulingStrategy splitSchedulingStrategy) {
=======
                                          ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy) {
>>>>>>> f773c602c... Test pr 10 (#27)

        int numSplits = this.pulsarConnectorConfig.getTargetNumSplits();

        PulsarTableLayoutHandle layoutHandle = (PulsarTableLayoutHandle) layout;
        PulsarTableHandle tableHandle = layoutHandle.getTable();
        TupleDomain<ColumnHandle> tupleDomain = layoutHandle.getTupleDomain();

<<<<<<< HEAD
        TopicName topicName = TopicName.get("persistent", NamespaceName.get(tableHandle.getSchemaName()),
                tableHandle.getTableName());

        SchemaInfo schemaInfo;
        try {
            schemaInfo = this.pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", tableHandle.getSchemaName(), tableHandle.getTableName()));
        } catch (PulsarAdminException e) {
            throw new RuntimeException("Failed to get schema for topic "
                    + String.format("%s/%s", tableHandle.getSchemaName(), tableHandle.getTableName())
                    + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
=======
        String namespace = restoreNamespaceDelimiterIfNeeded(tableHandle.getSchemaName(), pulsarConnectorConfig);
        TopicName topicName = TopicName.get("persistent", NamespaceName.get(namespace),
                tableHandle.getTableName());

        SchemaInfo schemaInfo;

        try {
            schemaInfo = this.pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", namespace, tableHandle.getTableName()));
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 401) {
                throw new PrestoException(QUERY_REJECTED,
                        String.format("Failed to get pulsar topic schema for topic %s/%s: Unauthorized",
                                namespace, tableHandle.getTableName()));
            } else if (e.getStatusCode() == 404) {
                schemaInfo = PulsarSchemaHandlers.defaultSchema();
            } else {
                throw new RuntimeException("Failed to get pulsar topic schema for topic "
                        + String.format("%s/%s", namespace, tableHandle.getTableName())
                        + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        Collection<PulsarSplit> splits;
        try {
<<<<<<< HEAD
            if (!PulsarConnectorUtils.isPartitionedTopic(topicName, this.pulsarAdmin)) {
                splits = getSplitsNonPartitionedTopic(numSplits, topicName, tableHandle, schemaInfo, tupleDomain);
                log.debug("Splits for non-partitioned topic %s: %s", topicName, splits);
            } else {
                splits = getSplitsPartitionedTopic(numSplits, topicName, tableHandle, schemaInfo, tupleDomain);
=======
            OffloadPolicies offloadPolicies = this.pulsarAdmin.namespaces()
                                                .getOffloadPolicies(topicName.getNamespace());
            if (!PulsarConnectorUtils.isPartitionedTopic(topicName, this.pulsarAdmin)) {
                splits = getSplitsNonPartitionedTopic(
                        numSplits, topicName, tableHandle, schemaInfo, tupleDomain, offloadPolicies);
                log.debug("Splits for non-partitioned topic %s: %s", topicName, splits);
            } else {
                splits = getSplitsPartitionedTopic(
                        numSplits, topicName, tableHandle, schemaInfo, tupleDomain, offloadPolicies);
>>>>>>> f773c602c... Test pr 10 (#27)
                log.debug("Splits for partitioned topic %s: %s", topicName, splits);
            }
        } catch (Exception e) {
            log.error(e, "Failed to get splits");
            throw new RuntimeException(e);
        }
        return new FixedSplitSource(splits);
    }

    @VisibleForTesting
<<<<<<< HEAD
    ManagedLedgerFactory getManagedLedgerFactory() throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setZkServers(this.pulsarConnectorConfig.getZookeeperUri())
                .setAllowShadedLedgerManagerFactoryClass(true)
                .setShadedLedgerManagerFactoryClassPrefix("org.apache.pulsar.shade.")
                .setClientTcpNoDelay(false)
                .setStickyReadsEnabled(true)
                .setUseV2WireProtocol(true);
        return new ManagedLedgerFactoryImpl(bkClientConfiguration);
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain) throws Exception {
        int numPartitions;
        try {
            numPartitions = (this.pulsarAdmin.topics().getPartitionedTopicMetadata(topicName.toString())).partitions;
        } catch (PulsarAdminException e) {
            throw new RuntimeException("Failed to get metadata for partitioned topic "
                    + topicName + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(),e);
        }

        int actualNumSplits = Math.max(numPartitions, numSplits);

        int splitsPerPartition = actualNumSplits / numPartitions;

        int splitRemainder = actualNumSplits % numPartitions;

        ManagedLedgerFactory managedLedgerFactory = getManagedLedgerFactory();

        try {
            List<PulsarSplit> splits = new LinkedList<>();
            for (int i = 0; i < numPartitions; i++) {

                int splitsForThisPartition = (splitRemainder > i) ? splitsPerPartition + 1 : splitsPerPartition;
                splits.addAll(
                        getSplitsForTopic(
                                topicName.getPartition(i).getPersistenceNamingEncoding(),
                                managedLedgerFactory,
                                splitsForThisPartition,
                                tableHandle,
                                schemaInfo,
                                topicName.getPartition(i).getLocalName(),
                                tupleDomain)
                );
            }
            return splits;
        } finally {
            if (managedLedgerFactory != null) {
                try {
                    managedLedgerFactory.shutdown();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsNonPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain) throws Exception {
        ManagedLedgerFactory managedLedgerFactory = null;
        try {
            managedLedgerFactory = getManagedLedgerFactory();

            return getSplitsForTopic(
                    topicName.getPersistenceNamingEncoding(),
                    managedLedgerFactory,
                    numSplits,
                    tableHandle,
                    schemaInfo,
                    tableHandle.getTableName(), tupleDomain);
        } finally {
            if (managedLedgerFactory != null) {
                try {
                    managedLedgerFactory.shutdown();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
=======
    Collection<PulsarSplit> getSplitsPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain,
              OffloadPolicies offloadPolicies) throws Exception {

        List<Integer> predicatedPartitions = getPredicatedPartitions(topicName, tupleDomain);
        if (log.isDebugEnabled()) {
            log.debug("Partition filter result %s", predicatedPartitions);
        }

        int actualNumSplits = Math.max(predicatedPartitions.size(), numSplits);

        int splitsPerPartition = actualNumSplits / predicatedPartitions.size();

        int splitRemainder = actualNumSplits % predicatedPartitions.size();

        ManagedLedgerFactory managedLedgerFactory = PulsarConnectorCache.getConnectorCache(pulsarConnectorConfig)
                .getManagedLedgerFactory();

        List<PulsarSplit> splits = new LinkedList<>();
        for (int i = 0; i < predicatedPartitions.size(); i++) {
            int splitsForThisPartition = (splitRemainder > i) ? splitsPerPartition + 1 : splitsPerPartition;
            splits.addAll(
                getSplitsForTopic(
                    topicName.getPartition(predicatedPartitions.get(i)).getPersistenceNamingEncoding(),
                    managedLedgerFactory,
                    splitsForThisPartition,
                    tableHandle,
                    schemaInfo,
                    topicName.getPartition(predicatedPartitions.get(i)).getLocalName(),
                    tupleDomain,
                    offloadPolicies));
        }
        return splits;
    }

    private List<Integer> getPredicatedPartitions(TopicName topicName, TupleDomain<ColumnHandle> tupleDomain) {
        int numPartitions;
        try {
            numPartitions = (this.pulsarAdmin.topics().getPartitionedTopicMetadata(topicName.toString())).partitions;
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 401) {
                throw new PrestoException(QUERY_REJECTED,
                    String.format("Failed to get metadata for partitioned topic %s: Unauthorized", topicName));
            }

            throw new RuntimeException("Failed to get metadata for partitioned topic "
                + topicName + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
        }
        List<Integer> predicatePartitions = new ArrayList<>();
        if (tupleDomain.getDomains().isPresent()) {
            Domain domain = tupleDomain.getDomains().get().get(PulsarInternalColumn.PARTITION
                .getColumnHandle(connectorId, false));
            if (domain != null) {
                domain.getValues().getValuesProcessor().consume(
                    ranges -> domain.getValues().getRanges().getOrderedRanges().forEach(range -> {
                        Integer low = 0;
                        Integer high = numPartitions;
                        if (!range.getLow().isLowerUnbounded() && range.getLow().getValueBlock().isPresent()) {
                            low = range.getLow().getValueBlock().get().getInt(0, 0);
                        }
                        if (!range.getHigh().isLowerUnbounded() && range.getHigh().getValueBlock().isPresent()) {
                            high = range.getHigh().getValueBlock().get().getInt(0, 0);
                        }
                        for (int i = low; i <= high; i++) {
                            predicatePartitions.add(i);
                        }
                    }),
                    discreteValues -> {},
                    allOrNone -> {});
            } else {
                for (int i = 0; i < numPartitions; i++) {
                    predicatePartitions.add(i);
                }
            }
        } else {
            for (int i = 0; i < numPartitions; i++) {
                predicatePartitions.add(i);
            }
        }
        return predicatePartitions;
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsNonPartitionedTopic(int numSplits, TopicName topicName,
            PulsarTableHandle tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain,
             OffloadPolicies offloadPolicies) throws Exception {
        ManagedLedgerFactory managedLedgerFactory = PulsarConnectorCache.getConnectorCache(pulsarConnectorConfig)
                .getManagedLedgerFactory();

        return getSplitsForTopic(
                topicName.getPersistenceNamingEncoding(),
                managedLedgerFactory,
                numSplits,
                tableHandle,
                schemaInfo,
                tableHandle.getTableName(),
                tupleDomain,
                offloadPolicies);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsForTopic(String topicNamePersistenceEncoding,
                                              ManagedLedgerFactory managedLedgerFactory,
                                              int numSplits,
                                              PulsarTableHandle tableHandle,
                                              SchemaInfo schemaInfo, String tableName,
<<<<<<< HEAD
                                              TupleDomain<ColumnHandle> tupleDomain)
            throws ManagedLedgerException, InterruptedException {
=======
                                              TupleDomain<ColumnHandle> tupleDomain,
                                              OffloadPolicies offloadPolicies)
            throws ManagedLedgerException, InterruptedException, IOException {
>>>>>>> f773c602c... Test pr 10 (#27)

        ReadOnlyCursor readOnlyCursor = null;
        try {
            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                    topicNamePersistenceEncoding,
                    PositionImpl.earliest, new ManagedLedgerConfig());

            long numEntries = readOnlyCursor.getNumberOfEntries();
            if (numEntries <= 0) {
                return Collections.EMPTY_LIST;
            }

            PredicatePushdownInfo predicatePushdownInfo = PredicatePushdownInfo.getPredicatePushdownInfo(
                    this.connectorId,
                    tupleDomain,
                    managedLedgerFactory,
                    topicNamePersistenceEncoding,
                    numEntries);

            PositionImpl initialStartPosition;
            if (predicatePushdownInfo != null) {
                numEntries = predicatePushdownInfo.getNumOfEntries();
                initialStartPosition = predicatePushdownInfo.getStartPosition();
            } else {
                initialStartPosition = (PositionImpl) readOnlyCursor.getReadPosition();
            }


            readOnlyCursor.close();
            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                    topicNamePersistenceEncoding,
                    initialStartPosition, new ManagedLedgerConfig());

            long remainder = numEntries % numSplits;

            long avgEntriesPerSplit = numEntries / numSplits;

            List<PulsarSplit> splits = new LinkedList<>();
            for (int i = 0; i < numSplits; i++) {
                long entriesForSplit = (remainder > i) ? avgEntriesPerSplit + 1 : avgEntriesPerSplit;
                PositionImpl startPosition = (PositionImpl) readOnlyCursor.getReadPosition();
                readOnlyCursor.skipEntries(Math.toIntExact(entriesForSplit));
                PositionImpl endPosition = (PositionImpl) readOnlyCursor.getReadPosition();

<<<<<<< HEAD
                splits.add(new PulsarSplit(i, this.connectorId,
                        tableHandle.getSchemaName(),
                        tableName,
                        entriesForSplit,
                        new String(schemaInfo.getSchema()),
=======
                PulsarSplit pulsarSplit = new PulsarSplit(i, this.connectorId,
                        restoreNamespaceDelimiterIfNeeded(tableHandle.getSchemaName(), pulsarConnectorConfig),
                        schemaInfo.getName(),
                        tableName,
                        entriesForSplit,
                        new String(schemaInfo.getSchema(),  "ISO8859-1"),
>>>>>>> f773c602c... Test pr 10 (#27)
                        schemaInfo.getType(),
                        startPosition.getEntryId(),
                        endPosition.getEntryId(),
                        startPosition.getLedgerId(),
                        endPosition.getLedgerId(),
<<<<<<< HEAD
                        tupleDomain));
=======
                        tupleDomain,
                        objectMapper.writeValueAsString(schemaInfo.getProperties()),
                        offloadPolicies);
                splits.add(pulsarSplit);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            return splits;
        } finally {
            if (readOnlyCursor != null) {
                try {
                    readOnlyCursor.close();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @Data
    private static class PredicatePushdownInfo {
        private PositionImpl startPosition;
        private PositionImpl endPosition;
        private long numOfEntries;

        private PredicatePushdownInfo(PositionImpl startPosition, PositionImpl endPosition, long numOfEntries) {
            this.startPosition = startPosition;
            this.endPosition = endPosition;
            this.numOfEntries = numOfEntries;
        }

        public static PredicatePushdownInfo getPredicatePushdownInfo(String connectorId,
                                                                     TupleDomain<ColumnHandle> tupleDomain,
                                                                     ManagedLedgerFactory managedLedgerFactory,
                                                                     String topicNamePersistenceEncoding,
                                                                     long totalNumEntries) throws
                ManagedLedgerException, InterruptedException {

            ReadOnlyCursor readOnlyCursor = null;
            try {
                readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                        topicNamePersistenceEncoding,
                        PositionImpl.earliest, new ManagedLedgerConfig());

                if (tupleDomain.getDomains().isPresent()) {
                    Domain domain = tupleDomain.getDomains().get().get(PulsarInternalColumn.PUBLISH_TIME
                            .getColumnHandle(connectorId, false));
                    if (domain != null) {
                        // TODO support arbitrary number of ranges
                        // only worry about one range for now
                        if (domain.getValues().getRanges().getRangeCount() == 1) {

                            checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

                            Long upperBoundTs = null;
                            Long lowerBoundTs = null;

                            Range range = domain.getValues().getRanges().getOrderedRanges().get(0);

                            if (!range.getHigh().isUpperUnbounded()) {
<<<<<<< HEAD
                                upperBoundTs = new SqlTimestampWithTimeZone(range.getHigh().getValueBlock().get()
                                        .getLong(0, 0)).getMillisUtc();
                            }

                            if (!range.getLow().isLowerUnbounded()) {
                                lowerBoundTs = new SqlTimestampWithTimeZone(range.getLow().getValueBlock().get()
                                        .getLong(0, 0)).getMillisUtc();
=======
                                upperBoundTs = new Timestamp(range.getHigh().getValueBlock().get()
                                        .getLong(0, 0)).getTime();
                            }

                            if (!range.getLow().isLowerUnbounded()) {
                                lowerBoundTs = new Timestamp(range.getLow().getValueBlock().get()
                                        .getLong(0, 0)).getTime();
>>>>>>> f773c602c... Test pr 10 (#27)
                            }

                            PositionImpl overallStartPos;
                            if (lowerBoundTs == null) {
                                overallStartPos = (PositionImpl) readOnlyCursor.getReadPosition();
                            } else {
                                overallStartPos = findPosition(readOnlyCursor, lowerBoundTs);
<<<<<<< HEAD
=======
                                if (overallStartPos == null) {
                                    overallStartPos = (PositionImpl) readOnlyCursor.getReadPosition();
                                }
>>>>>>> f773c602c... Test pr 10 (#27)
                            }

                            PositionImpl overallEndPos;
                            if (upperBoundTs == null) {
<<<<<<< HEAD

=======
>>>>>>> f773c602c... Test pr 10 (#27)
                                readOnlyCursor.skipEntries(Math.toIntExact(totalNumEntries));
                                overallEndPos = (PositionImpl) readOnlyCursor.getReadPosition();
                            } else {
                                overallEndPos = findPosition(readOnlyCursor, upperBoundTs);
<<<<<<< HEAD
=======
                                if (overallEndPos == null) {
                                    overallEndPos = overallStartPos;
                                }
>>>>>>> f773c602c... Test pr 10 (#27)
                            }

                            // Just use a close bound since presto can always filter out the extra entries even if
                            // the bound
                            // should be open or a mixture of open and closed
<<<<<<< HEAD
                            org.apache.pulsar.shade.com.google.common.collect.Range<PositionImpl> posRange
                                    = org.apache.pulsar.shade.com.google.common.collect.Range.range(overallStartPos,
                                    org.apache.pulsar.shade.com.google.common.collect.BoundType.CLOSED,
                                    overallEndPos, org.apache.pulsar.shade.com.google.common.collect.BoundType.CLOSED);

                            long numOfEntries = readOnlyCursor.getNumberOfEntries(posRange) - 1;

                            PredicatePushdownInfo predicatePushdownInfo
                                    = new PredicatePushdownInfo(overallStartPos, overallEndPos, numOfEntries);
=======
                            com.google.common.collect.Range<PositionImpl> posRange =
                                com.google.common.collect.Range.range(overallStartPos,
                                    com.google.common.collect.BoundType.CLOSED,
                                    overallEndPos, com.google.common.collect.BoundType.CLOSED);

                            long numOfEntries = readOnlyCursor.getNumberOfEntries(posRange) - 1;

                            PredicatePushdownInfo predicatePushdownInfo =
                                new PredicatePushdownInfo(overallStartPos, overallEndPos, numOfEntries);
>>>>>>> f773c602c... Test pr 10 (#27)
                            log.debug("Predicate pushdown optimization calculated: %s", predicatePushdownInfo);
                            return predicatePushdownInfo;
                        }
                    }
                }
            } finally {
                if (readOnlyCursor != null) {
                    readOnlyCursor.close();
                }
            }
            return null;
        }
    }

    private static PositionImpl findPosition(ReadOnlyCursor readOnlyCursor, long timestamp) throws
            ManagedLedgerException,
            InterruptedException {
        return (PositionImpl) readOnlyCursor.findNewestMatching(SearchAllAvailableEntries, new Predicate<Entry>() {
            @Override
            public boolean apply(Entry entry) {
                MessageImpl msg = null;
                try {
                    msg = MessageImpl.deserialize(entry.getDataBuffer());

                    return msg.getPublishTime() <= timestamp;
                } catch (Exception e) {
                    log.error(e, "Failed To deserialize message when finding position with error: %s", e);
                } finally {
                    entry.release();
                    if (msg != null) {
                        msg.recycle();
                    }
                }
                return false;
            }
        });
    }
}
