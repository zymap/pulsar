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
#ifndef JAVA_DEFAULT_HASH_HPP_
#define JAVA_DEFAULT_HASH_HPP_

<<<<<<< HEAD
=======
#include <pulsar/defines.h>
>>>>>>> f773c602c... Test pr 10 (#27)
#include "Hash.h"

#include <cstdint>
#include <string>
#include <boost/functional/hash.hpp>

<<<<<<< HEAD
#pragma GCC visibility push(default)
namespace pulsar {
class JavaStringHash : public Hash {
=======
namespace pulsar {
class PULSAR_PUBLIC JavaStringHash : public Hash {
>>>>>>> f773c602c... Test pr 10 (#27)
   public:
    JavaStringHash();
    int32_t makeHash(const std::string &key);
};
}  // namespace pulsar
<<<<<<< HEAD
#pragma GCC visibility pop
=======

>>>>>>> f773c602c... Test pr 10 (#27)
#endif /* JAVA_DEFAULT_HASH_HPP_ */
