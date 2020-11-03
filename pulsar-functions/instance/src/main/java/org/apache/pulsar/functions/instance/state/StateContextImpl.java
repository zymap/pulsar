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
package org.apache.pulsar.functions.instance.state;

import static java.nio.charset.StandardCharsets.UTF_8;
<<<<<<< HEAD
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.api.kv.Table;
=======

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.options.Options;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * This class accumulates the state updates from one function.
 *
 * <p>currently it exposes incr operations. but we can expose other key/values operations if needed.
 */
public class StateContextImpl implements StateContext {

    private final Table<ByteBuf, ByteBuf> table;

    public StateContextImpl(Table<ByteBuf, ByteBuf> table) {
        this.table = table;
    }

    @Override
<<<<<<< HEAD
    public void incr(String key, long amount) throws Exception {
        // TODO: this can be optimized with a batch operation.
        result(table.increment(
            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
            amount));
    }

    @Override
    public void put(String key, ByteBuffer value) throws Exception {
        result(table.put(
            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
            Unpooled.wrappedBuffer(value)));
    }

    @Override
    public ByteBuffer getValue(String key) throws Exception {
        ByteBuf data = result(table.get(Unpooled.wrappedBuffer(key.getBytes(UTF_8))));
        try {
            ByteBuffer result = ByteBuffer.allocate(data.readableBytes());
            data.readBytes(result);
            return result;
        } finally {
            data.release();
        }
    }

    @Override
    public long getAmount(String key) throws Exception {
        return result(table.getNumber(Unpooled.wrappedBuffer(key.getBytes(UTF_8))));
=======
    public CompletableFuture<Void> incrCounter(String key, long amount) {
        // TODO: this can be optimized with a batch operation.
        return table.increment(
            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
            amount);
    }

    @Override
    public CompletableFuture<Void> put(String key, ByteBuffer value) {
        if(value != null) {
            // Set position to off the buffer to the beginning.
            // If a user used an operation like ByteBuffer.allocate(4).putInt(count) to create a ByteBuffer to store to the state store
            // the position of the buffer will be at the end and nothing will be written to table service
            value.position(0);
            return table.put(
                    Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                    Unpooled.wrappedBuffer(value));
        } else {
            return table.put(
                    Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                    null);
        }
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return table.delete(
                Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                Options.delete()
        ).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<ByteBuffer> get(String key) {
        return table.get(Unpooled.wrappedBuffer(key.getBytes(UTF_8))).thenApply(
                data -> {
                    try {
                        if (data != null) {
                            ByteBuffer result = ByteBuffer.allocate(data.readableBytes());
                            data.readBytes(result);
                            // Set position to off the buffer to the beginning, since the position after the read is going to be end of the buffer
                            // If we do not rewind to the begining here, users will have to explicitly do this in their function code
                            // in order to use any of the ByteBuffer operations
                            result.position(0);
                            return result;
                        }
                        return null;
                    } finally {
                        ReferenceCountUtil.safeRelease(data);
                    }
                }
        );
    }

    @Override
    public CompletableFuture<Long> getCounter(String key) {
        return table.getNumber(Unpooled.wrappedBuffer(key.getBytes(UTF_8)));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

}
