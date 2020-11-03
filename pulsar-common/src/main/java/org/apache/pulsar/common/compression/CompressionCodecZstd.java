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
package org.apache.pulsar.common.compression;

<<<<<<< HEAD
import com.github.luben.zstd.Zstd;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
=======
import io.airlift.compress.zstd.ZStdRawCompressor;
import io.airlift.compress.zstd.ZStdRawDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
>>>>>>> f773c602c... Test pr 10 (#27)

import java.io.IOException;
import java.nio.ByteBuffer;

<<<<<<< HEAD
/**
 * Zstandard Compression
=======
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

/**
 * Zstandard Compression.
>>>>>>> f773c602c... Test pr 10 (#27)
 */
public class CompressionCodecZstd implements CompressionCodec {

    private static final int ZSTD_COMPRESSION_LEVEL = 3;

<<<<<<< HEAD
    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = (int) Zstd.compressBound(uncompressedLength);

        ByteBuf target = PooledByteBufAllocator.DEFAULT.directBuffer(maxLength, maxLength);
        int compressedLength;

        if (source.hasMemoryAddress()) {
            compressedLength = (int) Zstd.compressUnsafe(target.memoryAddress(), maxLength,
                    source.memoryAddress() + source.readerIndex(),
                    uncompressedLength, ZSTD_COMPRESSION_LEVEL);
=======
    private static final ZstdCompressor ZSTD_COMPRESSOR = new ZstdCompressor();

    private static final FastThreadLocal<ZStdRawDecompressor> ZSTD_RAW_DECOMPRESSOR = //
            new FastThreadLocal<ZStdRawDecompressor>() {
                @Override
                protected ZStdRawDecompressor initialValue() throws Exception {
                    return new ZStdRawDecompressor();
                }
            };

    private static final FastThreadLocal<ZstdDecompressor> ZSTD_DECOMPRESSOR = //
            new FastThreadLocal<ZstdDecompressor>() {
                @Override
                protected ZstdDecompressor initialValue() throws Exception {
                    return new ZstdDecompressor();
                }
            };

    @Override
    public ByteBuf encode(ByteBuf source) {
        int uncompressedLength = source.readableBytes();
        int maxLength = (int) ZSTD_COMPRESSOR.maxCompressedLength(uncompressedLength);

        ByteBuf target = PulsarByteBufAllocator.DEFAULT.buffer(maxLength, maxLength);
        int compressedLength;

        if (source.hasMemoryAddress() && target.hasMemoryAddress()) {
            compressedLength = ZStdRawCompressor.compress(
                    source.memoryAddress() + source.readerIndex(),
                    source.memoryAddress() + source.writerIndex(),
                    target.memoryAddress() + target.writerIndex(),
                    target.memoryAddress() + target.writerIndex() + maxLength,
                    ZSTD_COMPRESSION_LEVEL);
>>>>>>> f773c602c... Test pr 10 (#27)
        } else {
            ByteBuffer sourceNio = source.nioBuffer(source.readerIndex(), source.readableBytes());
            ByteBuffer targetNio = target.nioBuffer(0, maxLength);

<<<<<<< HEAD
            compressedLength = Zstd.compress(targetNio, sourceNio, ZSTD_COMPRESSION_LEVEL);
=======
            ZSTD_COMPRESSOR.compress(sourceNio, targetNio);
            compressedLength = targetNio.position();
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        target.writerIndex(compressedLength);
        return target;
    }

    @Override
    public ByteBuf decode(ByteBuf encoded, int uncompressedLength) throws IOException {
<<<<<<< HEAD
        ByteBuf uncompressed = PooledByteBufAllocator.DEFAULT.directBuffer(uncompressedLength, uncompressedLength);

        if (encoded.hasMemoryAddress()) {
            Zstd.decompressUnsafe(uncompressed.memoryAddress(), uncompressedLength,
                    encoded.memoryAddress() + encoded.readerIndex(),
                    encoded.readableBytes());
=======
        ByteBuf uncompressed = PulsarByteBufAllocator.DEFAULT.buffer(uncompressedLength, uncompressedLength);

        if (encoded.hasMemoryAddress() && uncompressed.hasMemoryAddress()) {
            ZSTD_RAW_DECOMPRESSOR.get().decompress(
                    null,
                    encoded.memoryAddress() + encoded.readerIndex(),
                    encoded.memoryAddress() + encoded.writerIndex(),
                    null,
                    uncompressed.memoryAddress() + uncompressed.writerIndex(),
                    uncompressed.memoryAddress() + uncompressed.writerIndex() + uncompressedLength);
>>>>>>> f773c602c... Test pr 10 (#27)
        } else {
            ByteBuffer uncompressedNio = uncompressed.nioBuffer(0, uncompressedLength);
            ByteBuffer encodedNio = encoded.nioBuffer(encoded.readerIndex(), encoded.readableBytes());

<<<<<<< HEAD
            Zstd.decompress(uncompressedNio, encodedNio);
=======
            ZSTD_DECOMPRESSOR.get().decompress(encodedNio, uncompressedNio);
>>>>>>> f773c602c... Test pr 10 (#27)
        }

        uncompressed.writerIndex(uncompressedLength);
        return uncompressed;
    }
}
