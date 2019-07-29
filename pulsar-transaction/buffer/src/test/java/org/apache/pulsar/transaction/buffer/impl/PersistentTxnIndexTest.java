/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.buffer.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats;
import org.testng.annotations.Test;

public class PersistentTxnIndexTest extends MockedBookKeeperTestCase {

    private Random randomGenerator = new Random(System.currentTimeMillis());

    @Test
    public void testTakeSnapshot() throws ManagedLedgerException, InterruptedException, ExecutionException,
                                          BKException {
        ManagedLedger txnlog = factory.open("test_takesnapshot");
        TransactionCursorImpl cursor = new TransactionCursorImpl(txnlog);
        List<TransactionMetaImpl> metaList = createExampleData();
        metaList.forEach(cursor::addToTxnIndex);
        List<TxnID> txnIDList = metaList.stream().map(TransactionMetaImpl::getTxnID).collect(Collectors.toList());
        cursor.takeSnapshot(PositionImpl.get(-1L, -1L)).get();

        LedgerHandle readLedger = cursor.getCursorLedger();
        assertEquals(readLedger.getLastAddConfirmed(), 22);

        Enumeration<LedgerEntry> entryList = readLedger.readEntries(0, readLedger.getLastAddConfirmed());

        int count = -1;
        while (entryList.hasMoreElements()) {
            if (count == -1) {
                entryList.nextElement();
                count++;
                continue;
            }
            LedgerEntry ledgerEntry = entryList.nextElement();
            byte[] data = ledgerEntry.getEntry();
            TransactionBufferDataFormats.StoredTxn txn = DataFormat.parseStoredTxn(data);
            if (count == 0) {
                assertEquals(txn.getStoredStatus(), TransactionBufferDataFormats.StoredStatus.START);
                assertEquals(txn.getPosition().getLedgerId(), -1L);
                assertEquals(txn.getPosition().getEntryId(), -1L);
                count++;
                continue;
            }

            if (count == metaList.size() + 1) {
                assertEquals(txn.getStoredStatus(), TransactionBufferDataFormats.StoredStatus.END);
                assertEquals(txn.getPosition().getLedgerId(), readLedger.getId());
                assertEquals(txn.getPosition().getEntryId(), 1);
                count++;
                continue;
            }
            assertEquals(txn.getStoredStatus(), TransactionBufferDataFormats.StoredStatus.MIDDLE);
            assertEquals(txn.getPosition().getLedgerId(), readLedger.getId());
            assertEquals(txn.getPosition().getEntryId(), 1);
            TransactionMetaImpl meta = (TransactionMetaImpl) DataFormat.parseToTransactionMeta(data);
            assertTrue(txnIDList.remove(meta.getTxnID()));
            count++;
        }
    }

    @Test
    public void testRecoverTxnIndex() throws ManagedLedgerException, InterruptedException {
        ManagedLedger txnLog = factory.open("test_recover_txnindex");
        TransactionCursorImpl cursor = new TransactionCursorImpl(txnLog);

        LedgerHandle write = cursor.getCursorLedger();
        write.addEntry()
    }

    private void writeExampleDataToLedger(LedgerHandle ledgerHandle) throws BKException, InterruptedException {
        TransactionBufferDataFormats.StoredTxn startStore = DataFormat.startStore(PositionImpl.get(-1, -1));
        long startEntryId = ledgerHandle.addEntry(startStore.toByteArray());

    }

    private List<TransactionMetaImpl> createExampleData() {
        List<TransactionMetaImpl> metaList = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            long mostBits = randomGenerator.nextInt(1000);
            long leastBits = randomGenerator.nextInt(1000);
            TxnID txnID = new TxnID(mostBits, leastBits);
            TransactionMetaImpl meta = new TransactionMetaImpl(txnID);
            metaList.add(meta);
        }
        return metaList;
    }

}
