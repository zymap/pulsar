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
package org.apache.bookkeeper.mledger.offload.jcloud;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreBackedInputStreamImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreManagedLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockUtils;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.JCloudBlobStoreProvider;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.jclouds.ContextBuilder;
import org.jclouds.azureblob.AzureBlobProviderMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class AZureBlobStoreTest {

    @Test
    public void testReadAzure() throws IOException, InterruptedException {

        Map<String, String> conf = new HashMap<>();
        conf.put("managedLedgerOffloadDriver", "azureblob");
        conf.put("managedLedgerOffloadBucket", "offload-performance");
        conf.put("AZURE_STORAGE_ACCOUNT", "offload");
        conf.put("AZURE_STORAGE_ACCESS_KEY", "7yu6SBNSd2ITIc79+vKXW234PRx2MKzxrASSg54h3J+zxcoWWZPJ/KxR/JXZ/ZfAoVdjLd9M5VlngCE0GJkfVw==");
        TieredStorageConfiguration tieredStorageConfiguration = new TieredStorageConfiguration(conf);
        BlobStore blobStore = tieredStorageConfiguration.getBlobStore();
        UUID uid = new UUID(7409457561646680139l, -7966195806851431593l);
        String bucket = "offload-performance";
        String key = DataBlockUtils.dataBlockOffloadKey(73, uid);
        String indexKey = DataBlockUtils.indexBlockOffloadKey(73, uid);
        DataBlockUtils.VersionCheck versionCheck = DataBlockUtils.VERSION_CHECK;

        Blob blob = blobStore.getBlob(bucket, indexKey);
        versionCheck.check(indexKey, blob);
        OffloadIndexBlockBuilder indexBlockBuilder = OffloadIndexBlockBuilder.create();
        OffloadIndexBlock index = indexBlockBuilder.fromStream(blob.getPayload().openStream());
        int bufferSize = 10240;
        BackedInputStream inputStream = new BlobStoreBackedInputStreamImpl(blobStore, bucket, key, versionCheck, index.getDataObjectLength(), bufferSize);

        int cursor = 0;
        int firstNumber = 0;
        for (int i = 0; i < bufferSize; i++) {
            int n = inputStream.read();
            if (i == 0) {
                firstNumber = n;
                System.out.println("cursor: " + (cursor + i) + " : " + n);
            }
        }
        inputStream.seek(cursor);
        for (int j = 0; j < bufferSize; j++) {
            int n = inputStream.read();
            if (j == 0) {
                Assert.assertEquals(n, firstNumber);
                System.out.println("cursor: " + (cursor + j) + " : " + n);
            }
        }
        cursor += bufferSize;
        for (int i = 0; i < bufferSize; i++) {
            int n = inputStream.read();
            if (i == 0) {
                System.out.println("cursor: " + (i) + " : " + n);
            }
        }

        cursor += bufferSize;
        for (int i = 0; i < bufferSize; i++) {
            int n = inputStream.read();
            if (i == 0) {
                System.out.println("cursor: " + (i) + " : " + n);
            }
        }

        System.out.println("--------------------------------------------------");
        inputStream.seek(0);
        for (int i = 0; i < bufferSize; i++) {
            int n = inputStream.read();
            if (i == 0) {
                System.out.println("cursor: " + (i) + " : " + n);
            }
        }
        System.out.println("---------------====================-------------==========");
        for (int i = 0; i < bufferSize; i++) {
            int n = inputStream.read();
            if (i == 0) {
                System.out.println("cursor: " + (i) + " : " + n);
            }
        }
//        cursor = 1025;
//        inputStream.seek(cursor);
//        System.out.println("seek to 1025");
//        for (int j = 0; j < 500 ; j++) {
//            System.out.println("cursor: " + (cursor + j) + " : " + inputStream.read());
//        }
//        cursor = 3000;
//        inputStream.seek(cursor);
//        System.out.println("seek to 3000");
//        for (int j = 0; j < 500 ; j++) {
//            System.out.println("cursor: " + (cursor + j) + " : " + inputStream.read());
//        }
//        cursor = 100;
//        inputStream.seek(cursor);
//        System.out.println("seek to 100");
//        for (int j = 0; j < 500 ; j++) {
//            System.out.println("cursor: " + (cursor + j) + " : " + inputStream.read());
//        }
    }

    @Test
    public void testCopyBytes() {
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(1024);
        ByteBuf cache = PulsarByteBufAllocator.DEFAULT.buffer(1024);

        buf.writeBytes(new byte[1024]);
        ByteBuf b = cache.writeBytes(buf);
        ByteBuf c = cache.readBytes(buf);
        System.out.println(b.readableBytes());
    }

    @Test
    public void testLock() throws InterruptedException {
        TestLock test = new TestLock();
        CountDownLatch latch = new CountDownLatch(2);

        new Thread(() -> {
            System.out.println(new Date(System.currentTimeMillis()));
            test.method();
            latch.countDown();
        }).start();

        TimeUnit.SECONDS.sleep(1);

        new Thread(() -> {
            System.out.println(new Date(System.currentTimeMillis()));
            test.method2();
            System.out.println(new Date(System.currentTimeMillis()));
            latch.countDown();
        }).start();

        latch.await();
    }

    static class TestLock{
        private ReentrantLock lock;
        private AtomicLong test;

        TestLock() {
            lock = new ReentrantLock();
            test = new AtomicLong(0);
        }

        void method() {
            lock.lock();
            try {
                TimeUnit.SECONDS.sleep(10);
                test.getAndAdd(10);
                System.out.println(test.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                lock.unlock();
            }
        }

        void method2() {
            test.set(100);
                System.out.println("get lock");
        }
    }
}
