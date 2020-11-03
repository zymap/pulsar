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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;

import io.netty.buffer.ByteBuf;
<<<<<<< HEAD
import io.netty.buffer.PooledByteBufAllocator;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

<<<<<<< HEAD
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.common.api.Commands;
=======
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;

public class RawBatchConverter {

    public static boolean isReadableBatch(RawMessage msg) {
        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        try {
            return metadata.hasNumMessagesInBatch() && metadata.getEncryptionKeysCount() == 0;
        } finally {
            metadata.recycle();
        }
    }

<<<<<<< HEAD
    public static List<ImmutablePair<MessageId,String>> extractIdsAndKeys(RawMessage msg)
=======
    public static List<ImmutableTriple<MessageId, String, Integer>> extractIdsAndKeysAndSize(RawMessage msg)
>>>>>>> f773c602c... Test pr 10 (#27)
            throws IOException {
        checkArgument(msg.getMessageIdData().getBatchIndex() == -1);

        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        int batchSize = metadata.getNumMessagesInBatch();

        CompressionType compressionType = metadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = metadata.getUncompressedSize();
        ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        metadata.recycle();

<<<<<<< HEAD
        List<ImmutablePair<MessageId,String>> idsAndKeys = new ArrayList<>();
=======
        List<ImmutableTriple<MessageId, String, Integer>> idsAndKeysAndSize = new ArrayList<>();
>>>>>>> f773c602c... Test pr 10 (#27)

        for (int i = 0; i < batchSize; i++) {
            SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata.newBuilder();
            ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                                                                                    singleMessageMetadataBuilder,
                                                                                    0, batchSize);
            MessageId id = new BatchMessageIdImpl(msg.getMessageIdData().getLedgerId(),
                                                  msg.getMessageIdData().getEntryId(),
                                                  msg.getMessageIdData().getPartition(),
                                                  i);
            if (!singleMessageMetadataBuilder.getCompactedOut()) {
<<<<<<< HEAD
                idsAndKeys.add(ImmutablePair.of(id, singleMessageMetadataBuilder.getPartitionKey()));
=======
                idsAndKeysAndSize.add(ImmutableTriple.of(id, singleMessageMetadataBuilder.getPartitionKey(), singleMessageMetadataBuilder.getPayloadSize()));
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            singleMessageMetadataBuilder.recycle();
            singleMessagePayload.release();
        }
        uncompressedPayload.release();
<<<<<<< HEAD
        return idsAndKeys;
=======
        return idsAndKeysAndSize;
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    /**
     * Take a batched message and a filter, and returns a message with the only the sub-messages
     * which match the filter. Returns an empty optional if no messages match.
     *
<<<<<<< HEAD
     * This takes ownership of the passes in message, and if the returned optional is not empty,
     * the ownership of that message is returned also.
=======
     *  NOTE: this message does not alter the reference count of the RawMessage argument.
>>>>>>> f773c602c... Test pr 10 (#27)
     */
    public static Optional<RawMessage> rebatchMessage(RawMessage msg,
                                                      BiPredicate<String, MessageId> filter)
            throws IOException {
        checkArgument(msg.getMessageIdData().getBatchIndex() == -1);

        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
<<<<<<< HEAD
        ByteBuf batchBuffer = PooledByteBufAllocator.DEFAULT.buffer(payload.capacity());
=======
        ByteBuf batchBuffer = PulsarByteBufAllocator.DEFAULT.buffer(payload.capacity());
>>>>>>> f773c602c... Test pr 10 (#27)

        CompressionType compressionType = metadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);

        int uncompressedSize = metadata.getUncompressedSize();
        ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
        try {
            int batchSize = metadata.getNumMessagesInBatch();
            int messagesRetained = 0;

            SingleMessageMetadata.Builder emptyMetadataBuilder = SingleMessageMetadata.newBuilder().setCompactedOut(true);
            for (int i = 0; i < batchSize; i++) {
                SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata.newBuilder();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                                                                                        singleMessageMetadataBuilder,
                                                                                        0, batchSize);
                MessageId id = new BatchMessageIdImpl(msg.getMessageIdData().getLedgerId(),
                                                      msg.getMessageIdData().getEntryId(),
                                                      msg.getMessageIdData().getPartition(),
                                                      i);
                if (!singleMessageMetadataBuilder.hasPartitionKey()) {
                    messagesRetained++;
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadataBuilder,
                                                                      singleMessagePayload, batchBuffer);
                } else if (filter.test(singleMessageMetadataBuilder.getPartitionKey(), id)
                           && singleMessagePayload.readableBytes() > 0) {
                    messagesRetained++;
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadataBuilder,
                                                                      singleMessagePayload, batchBuffer);
                } else {
                    Commands.serializeSingleMessageInBatchWithPayload(emptyMetadataBuilder,
                                                                      Unpooled.EMPTY_BUFFER, batchBuffer);
                }
                singleMessageMetadataBuilder.recycle();
                singleMessagePayload.release();
            }
            emptyMetadataBuilder.recycle();

            if (messagesRetained > 0) {
                int newUncompressedSize = batchBuffer.readableBytes();
                ByteBuf compressedPayload = codec.encode(batchBuffer);

                MessageMetadata.Builder metadataBuilder = metadata.toBuilder();
                metadataBuilder.setUncompressedSize(newUncompressedSize);
                MessageMetadata newMetadata = metadataBuilder.build();

                ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                                                                                  newMetadata, compressedPayload);
                Optional<RawMessage> result = Optional.of(new RawMessageImpl(msg.getMessageIdData(),
                                                                             metadataAndPayload));
                metadataBuilder.recycle();
                newMetadata.recycle();
                metadataAndPayload.release();
                compressedPayload.release();
                return result;
            } else {
                return Optional.empty();
            }
        } finally {
<<<<<<< HEAD
            batchBuffer.release();
            metadata.recycle();
            msg.close();
=======
            uncompressedPayload.release();
            batchBuffer.release();
            metadata.recycle();
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }
}
