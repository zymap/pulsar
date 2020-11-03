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
#include "CompressionCodec.h"
#include "CompressionCodecLZ4.h"
#include "CompressionCodecZLib.h"
#include "CompressionCodecZstd.h"
<<<<<<< HEAD
=======
#include "CompressionCodecSnappy.h"
>>>>>>> f773c602c... Test pr 10 (#27)

#include <cassert>

using namespace pulsar;
namespace pulsar {

CompressionCodecNone CompressionCodecProvider::compressionCodecNone_;
CompressionCodecLZ4 CompressionCodecProvider::compressionCodecLZ4_;
CompressionCodecZLib CompressionCodecProvider::compressionCodecZLib_;
CompressionCodecZstd CompressionCodecProvider::compressionCodecZstd_;
<<<<<<< HEAD
=======
CompressionCodecSnappy CompressionCodecProvider::compressionCodecSnappy_;
>>>>>>> f773c602c... Test pr 10 (#27)

CompressionCodec& CompressionCodecProvider::getCodec(CompressionType compressionType) {
    switch (compressionType) {
        case CompressionLZ4:
            return compressionCodecLZ4_;
        case CompressionZLib:
            return compressionCodecZLib_;
        case CompressionZSTD:
            return compressionCodecZstd_;
<<<<<<< HEAD
=======
        case CompressionSNAPPY:
            return compressionCodecSnappy_;
>>>>>>> f773c602c... Test pr 10 (#27)
        default:
            return compressionCodecNone_;
    }
}

CompressionType CompressionCodecProvider::convertType(proto::CompressionType type) {
    switch (type) {
        case proto::NONE:
            return CompressionNone;
        case proto::LZ4:
            return CompressionLZ4;
        case proto::ZLIB:
            return CompressionZLib;
        case proto::ZSTD:
            return CompressionZSTD;
<<<<<<< HEAD
=======
        case proto::SNAPPY:
            return CompressionSNAPPY;
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}

proto::CompressionType CompressionCodecProvider::convertType(CompressionType type) {
    switch (type) {
        case CompressionNone:
            return proto::NONE;
        case CompressionLZ4:
            return proto::LZ4;
        case CompressionZLib:
            return proto::ZLIB;
        case CompressionZSTD:
            return proto::ZSTD;
<<<<<<< HEAD
=======
        case CompressionSNAPPY:
            return proto::SNAPPY;
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}

SharedBuffer CompressionCodecNone::encode(const SharedBuffer& raw) { return raw; }

bool CompressionCodecNone::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                  SharedBuffer& decoded) {
    decoded = encoded;
    return true;
}
}  // namespace pulsar
