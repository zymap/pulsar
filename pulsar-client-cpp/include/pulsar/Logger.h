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
#pragma once

#include <memory>
<<<<<<< HEAD

#pragma GCC visibility push(default)

namespace pulsar {

class Logger {
   public:
    enum Level
    {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3
=======
#include <string>
#include <pulsar/defines.h>

namespace pulsar {

class PULSAR_PUBLIC Logger {
   public:
    enum Level
    {
        LEVEL_DEBUG = 0,
        LEVEL_INFO = 1,
        LEVEL_WARN = 2,
        LEVEL_ERROR = 3
>>>>>>> f773c602c... Test pr 10 (#27)
    };

    virtual ~Logger() {}

    virtual bool isEnabled(Level level) = 0;

    virtual void log(Level level, int line, const std::string& message) = 0;
};

<<<<<<< HEAD
class LoggerFactory {
=======
class PULSAR_PUBLIC LoggerFactory {
>>>>>>> f773c602c... Test pr 10 (#27)
   public:
    virtual ~LoggerFactory() {}

    virtual Logger* getLogger(const std::string& fileName) = 0;
};

<<<<<<< HEAD
typedef std::shared_ptr<LoggerFactory> LoggerFactoryPtr;
}  // namespace pulsar
#pragma GCC visibility pop
=======
}  // namespace pulsar
>>>>>>> f773c602c... Test pr 10 (#27)
