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

<<<<<<< HEAD
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
=======
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
<<<<<<< HEAD

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.junit.Before;
import org.junit.Test;
=======
import static org.testng.AssertJUnit.assertTrue;
>>>>>>> f773c602c... Test pr 10 (#27)

/**
 * Unit test {@link StateContextImpl}.
 */
public class StateContextImplTest {

    private Table<ByteBuf, ByteBuf> mockTable;
    private StateContextImpl stateContext;

<<<<<<< HEAD
    @Before
=======
    @BeforeMethod
>>>>>>> f773c602c... Test pr 10 (#27)
    public void setup() {
        this.mockTable = mock(Table.class);
        this.stateContext = new StateContextImpl(mockTable);
    }

    @Test
    public void testIncr() throws Exception {
        when(mockTable.increment(any(ByteBuf.class), anyLong()))
            .thenReturn(FutureUtils.Void());
<<<<<<< HEAD
        stateContext.incr("test-key", 10L);
=======
        stateContext.incrCounter("test-key", 10L).get();
>>>>>>> f773c602c... Test pr 10 (#27)
        verify(mockTable, times(1)).increment(
            eq(Unpooled.copiedBuffer("test-key", UTF_8)),
            eq(10L)
        );
    }

    @Test
    public void testPut() throws Exception {
        when(mockTable.put(any(ByteBuf.class), any(ByteBuf.class)))
            .thenReturn(FutureUtils.Void());
<<<<<<< HEAD
        stateContext.put("test-key", ByteBuffer.wrap("test-value".getBytes(UTF_8)));
=======
        stateContext.put("test-key", ByteBuffer.wrap("test-value".getBytes(UTF_8))).get();
>>>>>>> f773c602c... Test pr 10 (#27)
        verify(mockTable, times(1)).put(
            eq(Unpooled.copiedBuffer("test-key", UTF_8)),
            eq(Unpooled.copiedBuffer("test-value", UTF_8))
        );
    }

    @Test
<<<<<<< HEAD
=======
    public void testDelete() throws Exception {
        DeleteResult<ByteBuf, ByteBuf> result = mock(DeleteResult.class);
        when(mockTable.delete(any(ByteBuf.class), eq(Options.delete())))
                .thenReturn(FutureUtils.value(result));
        stateContext.delete("test-key");
        verify(mockTable, times(1)).delete(
                eq(Unpooled.copiedBuffer("test-key", UTF_8)),
                eq(Options.delete())
        );
    }

    @Test
>>>>>>> f773c602c... Test pr 10 (#27)
    public void testGetValue() throws Exception {
        ByteBuf returnedValue = Unpooled.copiedBuffer("test-value", UTF_8);
        when(mockTable.get(any(ByteBuf.class)))
            .thenReturn(FutureUtils.value(returnedValue));
<<<<<<< HEAD
        ByteBuffer result = stateContext.getValue("test-key");
=======
        ByteBuffer result = stateContext.get("test-key").get();
>>>>>>> f773c602c... Test pr 10 (#27)
        assertEquals("test-value", new String(result.array(), UTF_8));
        verify(mockTable, times(1)).get(
            eq(Unpooled.copiedBuffer("test-key", UTF_8))
        );
    }

    @Test
    public void testGetAmount() throws Exception {
        when(mockTable.getNumber(any(ByteBuf.class)))
            .thenReturn(FutureUtils.value(10L));
<<<<<<< HEAD
        assertEquals(10L, stateContext.getAmount("test-key"));
=======
        assertEquals((Long)10L, stateContext.getCounter("test-key").get());
>>>>>>> f773c602c... Test pr 10 (#27)
        verify(mockTable, times(1)).getNumber(
            eq(Unpooled.copiedBuffer("test-key", UTF_8))
        );
    }

<<<<<<< HEAD
=======
    @Test
    public void testGetKeyNotPresent() throws Exception {
        when(mockTable.get(any(ByteBuf.class)))
                .thenReturn(FutureUtils.value(null));
        CompletableFuture<ByteBuffer> result = stateContext.get("test-key");
        assertTrue(result != null);
        assertEquals(result.get(), null);

    }

>>>>>>> f773c602c... Test pr 10 (#27)
}
