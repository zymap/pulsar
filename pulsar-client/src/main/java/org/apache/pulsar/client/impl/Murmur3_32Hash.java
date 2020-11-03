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
/*
 * The original MurmurHash3 was written by Austin Appleby, and is placed in the
 * public domain. This source code, implemented by Licht Takeuchi, is based on
 * the orignal MurmurHash3 source code.
 */
package org.apache.pulsar.client.impl;

<<<<<<< HEAD
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import com.google.common.primitives.UnsignedBytes;

public class Murmur3_32Hash implements Hash {
    private static final Murmur3_32Hash instance = new Murmur3_32Hash();

    private static final int CHUNK_SIZE = 4;
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private final int seed;

    private Murmur3_32Hash() {
        seed = 0;
    }
=======
import java.nio.charset.StandardCharsets;

public class Murmur3_32Hash implements Hash {
    private static final Murmur3_32Hash instance = new Murmur3_32Hash();

    private Murmur3_32Hash(){ }
>>>>>>> f773c602c... Test pr 10 (#27)

    public static Hash getInstance() {
        return instance;
    }

    @Override
    public int makeHash(String s) {
<<<<<<< HEAD
        return makeHash(s.getBytes(StandardCharsets.UTF_8)) & Integer.MAX_VALUE;
    }

    private int makeHash(byte[] bytes) {
        int len = bytes.length;
        int reminder = len % CHUNK_SIZE;
        int h1 = seed;

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        while (byteBuffer.remaining() >= CHUNK_SIZE) {
            int k1 = byteBuffer.getInt();

            k1 = mixK1(k1);
            h1 = mixH1(h1, k1);
        }

        int k1 = 0;
        for (int i = 0; i < reminder; i++) {
            k1 ^= UnsignedBytes.toInt(byteBuffer.get()) << (i * 8);
        }

        h1 ^= mixK1(k1);
        h1 ^= len;
        h1 = fmix(h1);

        return h1;
    }

    private int fmix(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;

        return h;
    }

    private int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        return h1 * 5 + 0xe6546b64;
=======
        return org.apache.pulsar.common.util.Murmur3_32Hash.getInstance()
            .makeHash(s.getBytes(StandardCharsets.UTF_8)) & Integer.MAX_VALUE;
    }

    @Override
    public int makeHash(byte[] b) {
        return org.apache.pulsar.common.util.Murmur3_32Hash.getInstance().makeHash(b) & Integer.MAX_VALUE;
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}
