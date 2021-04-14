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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockUtils.VersionCheck;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreBackedInputStreamImpl extends BackedInputStream {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreBackedInputStreamImpl.class);

    private final BlobStore blobStore;
    private final String bucket;
    private final String key;
    private final VersionCheck versionCheck;
    private ByteBuf buffer;
    private final long objectLen;
    private final int bufferSize;

    private long cursor;
    private long bufferOffsetStart;
    private long bufferOffsetEnd;

    private ReadProcessorV1 readProcessor;

    public BlobStoreBackedInputStreamImpl(BlobStore blobStore, String bucket, String key,
                                          VersionCheck versionCheck,
                                          long objectLen, int bufferSize) {
        this.blobStore = blobStore;
        this.bucket = bucket;
        this.key = key;
        this.versionCheck = versionCheck;
        this.buffer = PulsarByteBufAllocator.DEFAULT.buffer(bufferSize, bufferSize);
        this.objectLen = objectLen;
        this.bufferSize = bufferSize;
        this.cursor = 0;
        this.bufferOffsetStart = this.bufferOffsetEnd = -1;
        if (log.isDebugEnabled()) {
            log.debug("Prepare read processor for reading data from tiered storage");
        }
        this.readProcessor = new ReadProcessorV1(blobStore, bucket, key, versionCheck, objectLen, bufferSize);
        readProcessor.startingWriteProcessor();
    }

    private boolean fillBufferFromCache() throws IOException {
        if (buffer.readableBytes() == 0) {
            if (cursor > objectLen) {
                return false;
            }
            buffer.clear();
            bufferOffsetStart = cursor;
            bufferOffsetEnd = Math.min(cursor + bufferSize - 1, objectLen - 1);;
            ByteBuf readedBuf = readProcessor.getBuffer();
            buffer.writeBytes(readedBuf);
            readedBuf.release();
            cursor += buffer.readableBytes();
            if (log.isDebugEnabled()) {
                log.debug("buffer is empty, trying to get a new buffer to read. cursor: {}, begin: {}, end: {}", cursor, bufferOffsetStart, bufferOffsetEnd);
            }
        }
        return true;
    }

    /**
     * Refill the buffered input if it is empty.
     * @return true if there are bytes to read, false otherwise
     */
    private boolean refillBufferIfNeeded() throws IOException {
        if (buffer.readableBytes() == 0) {
            if (cursor >= objectLen) {
                return false;
            }
            long startRange = cursor;
            long endRange = Math.min(cursor + bufferSize - 1,
                                     objectLen - 1);

            try {
                Blob blob = blobStore.getBlob(bucket, key, new GetOptions().range(startRange, endRange));
                versionCheck.check(key, blob);

                try (InputStream stream = blob.getPayload().openStream()) {
                    buffer.clear();
                    bufferOffsetStart = startRange;
                    bufferOffsetEnd = endRange;
                    long bytesRead = endRange - startRange + 1;
                    int bytesToCopy = (int) bytesRead;
                    while (bytesToCopy > 0) {
                        bytesToCopy -= buffer.writeBytes(stream, bytesToCopy);
                    }
                    cursor += buffer.readableBytes();
                }
            } catch (Throwable e) {
                throw new IOException("Error reading from BlobStore", e);
            }
        }
        return true;
    }

    private ByteBuf getBuffer() throws IOException {
        if (buffer == null || buffer.readableBytes() == 0) {
            if (log.isDebugEnabled()) {
                log.debug("buffer is empty, trying to get a new buffer to read");
            }
            bufferOffsetStart = cursor;
            buffer = readProcessor.getBuffer();
            cursor += buffer.readableBytes();
            bufferOffsetEnd += buffer.readableBytes();
        }
        return buffer;
    }

    @Override
    public int read() throws IOException {
        int returned = -1;
        if (fillBufferFromCache()) {
            returned = buffer.readUnsignedByte();
        }
        if (log.isDebugEnabled()) {
            log.debug("reading from the buffer: read index {}. result {}", buffer.readerIndex(), returned);
        }
        return returned;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = -1;
        if (fillBufferFromCache()) {
            int bytesToRead = Math.min(len, buffer.readableBytes());
            buffer.readBytes(b, off, bytesToRead);
            result = bytesToRead;
        }
        if (log.isDebugEnabled()) {
            log.debug("reading form the given position, offset {}, length: {}. buffer: read index {}. result {}", off, len, buffer.readerIndex(), result);
        }
        return result;
    }

    @Override
    public void seek(long position) {
        if (log.isDebugEnabled()) {
            log.debug("Seeking to {} on {}/{}, current position {} (bufStart:{}, bufEnd:{})",
                position, bucket, key, cursor, bufferOffsetStart, bufferOffsetEnd);
        }
        if (position >= bufferOffsetStart && position <= bufferOffsetEnd) {
            long newIndex = position - bufferOffsetStart;
            buffer.readerIndex((int) newIndex);
            if (log.isDebugEnabled()) {
                log.debug("Current buffer info: buffer reader index position: {}, available read {}",
                    buffer.readerIndex(), buffer.readableBytes());
            }
        } else {
            this.cursor = position;
            readProcessor.seek(position);
            try {
                fillBufferFromCache();
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.clear();
        }
    }

    @Override
    public void seekForward(long position) throws IOException {
        if (position >= cursor) {
            seek(position);
        } else {
            throw new IOException(String.format("Error seeking, new position %d < current position %d",
                                                position, cursor));
        }
    }

    @Override
    public void close() {
        readProcessor.close();
        buffer.release();
    }

    static class ReadProcessorV1 {
        private final BlobStore blobStore;
        private final String bucket;
        private final String key;
        private final VersionCheck versionCheck;
        private final long objectLen;
        private final int bufferSize;

        private BufferCache bufferCache;
        private CompletableFuture<Void> future = new CompletableFuture<>();

        private AtomicLong cursor;
        private AtomicBoolean seeking = new AtomicBoolean(false);

        ReadProcessorV1(BlobStore blobStore, String bucket, String key,
                        VersionCheck versionCheck,
                        long objectLen, int bufferSize) {
            this.blobStore = blobStore;
            this.bucket = bucket;
            this.key = key;
            this.versionCheck = versionCheck;
            this.objectLen = objectLen;
            this.bufferSize = bufferSize;
            this.bufferCache = new BufferCache(bufferSize, 2);
            this.cursor = new AtomicLong(0);
        }

        void seek(long cursorPosition) {
            if (log.isDebugEnabled()) {
                log.debug("Read processor seeking to the position: {}, current cursor position {}", cursorPosition, cursor.get());
            }
            seeking.set(true);
            cursor.set(cursorPosition);
            bufferCache.clear();
            seeking.set(false);
        }

        boolean fillBuffer() {
            if (seeking.get()) {
                return true;
            }
                if (log.isDebugEnabled()) {
                    log.debug("Filling cached buffer for reading from cursor: {}", cursor.get());
                }
                long currentCursor = cursor.get();
                if (currentCursor > objectLen) {
                    future.complete(null);
                    return false;
                }
                long startRange = currentCursor;
                long endRange = Math.min(currentCursor + bufferSize - 1, objectLen - 1);
                try {
                    Blob blob = blobStore.getBlob(bucket, key, new GetOptions().range(startRange, endRange));
                    versionCheck.check(key, blob);

                    try (InputStream stream = blob.getPayload().openStream()) {
                        long bytesRead = endRange - startRange + 1;
                        int bytesToCopy = (int) bytesRead;
                        int copedBytes = bufferCache.write(stream, bytesToCopy);
                        cursor.compareAndSet(currentCursor, currentCursor + copedBytes);
                    }
                } catch (Throwable e) {
                    log.error("Error reading from blobstore", e);
                    future.completeExceptionally(e);
                    return false;
                }
                return true;
        }

        ByteBuf getBuffer() throws IOException {
            if (future.isDone()) {
                if (log.isDebugEnabled()) {
                    log.debug("The stream is reading out. done the read");
                }
                return null;
            }
            if (future.isCompletedExceptionally()) {
                if (log.isDebugEnabled()) {
                    log.debug("filling buffer process has encounter the error");
                }
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                } catch (ExecutionException e) {
                    throw new IOException(e.getCause());
                }
            }
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Getting the next buffer to read");
                }
                return bufferCache.getNextReadBuffer();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        void startingWriteProcessor() {
            new Thread(() -> {
                while (fillBuffer()) {
                    log.info("Filling buffer for reading... cursor: {}", cursor);
                }
            }).start();
        }

        void close() {
            bufferCache.close();
        }
    }

    static class BufferCache {
        private SharedBuffer[] buffer;
        private static int writeCount = 0;
        private static int readCount = 0;
        private final int num;

        BufferCache(int bufferSize, int num) {
            this.buffer = new SharedBuffer[num];
            this.num = num;
            for (int i = 0; i < buffer.length; i++) {
                buffer[i] = new SharedBuffer(bufferSize);
            }
        }

        void clear() {
            if (log.isDebugEnabled()) {
                log.debug("clear the buffer cache for resetting the read");
            }
            for (int i = 0; i < buffer.length; i++) {
                buffer[i].clear();
            }
            writeCount = 0;
            readCount = 0;
        }

        ByteBuf getNextReadBuffer() throws InterruptedException {
            if (log.isDebugEnabled()) {
                log.debug("read from the buffer {}", readCount);
            }
            return buffer[readCount++ % num].read();
        }

        int write(InputStream inputStream, int bytesToWrite) throws IOException, InterruptedException {
            if (log.isDebugEnabled()) {
                log.debug("write to the buffer {}", writeCount);
            }
            return buffer[writeCount++ % num].write(inputStream, bytesToWrite);
        }

        void close() {
            for (int i = 0; i < buffer.length; i++) {
                buffer[i].close();
            }
        }
    }

    static class SharedBuffer {
        private final ByteBuf buf;
        private final ReentrantLock lock;
        private final Condition canRead;
        private final Condition canWrite;

        public SharedBuffer(int bufferSize) {
            this.buf = PulsarByteBufAllocator.DEFAULT.buffer(bufferSize);
            this.lock = new ReentrantLock();
            this.canRead = lock.newCondition();
            this.canWrite = lock.newCondition();
        }

        public int write(InputStream inputStream, int bytesToWrite) throws IOException, InterruptedException {
            lock.lock();
            try {
                while (buf.writableBytes() == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("buffer already full, waiting for the signal to release");
                    }
                    canWrite.await();
                }
                if (log.isDebugEnabled()) {
                    log.debug("buffer get the write signal, continue to write");
                }
                buf.clear();
                while (bytesToWrite > 0) {
                    bytesToWrite -= buf.writeBytes(inputStream, bytesToWrite);
                }
                return buf.readableBytes();
            } finally {
                canRead.signal();
                lock.unlock();
            }
        }

        public ByteBuf read() throws InterruptedException {
            lock.lock();
            try {
                while (buf.readableBytes() == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("buffer can not read, waiting for the read signal");
                    }
                    canRead.await();
                }
                if (log.isDebugEnabled()) {
                    log.debug("Buffer get the read signal, continue to read");
                }
                ByteBuf returnedBuf = buf.copy();
                buf.clear();
                return returnedBuf;
            }finally {
                canWrite.signal();
                lock.unlock();
            }
        }

        public void clear() {
            if (log.isDebugEnabled()) {
                log.debug("Clean the buffer");
            }
            buf.clear();
        }

        void close() {
            buf.release();
        }
    }
}
