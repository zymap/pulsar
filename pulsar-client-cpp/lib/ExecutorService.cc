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
#include "ExecutorService.h"

#include <boost/asio.hpp>
#include <functional>
#include <memory>

namespace pulsar {

ExecutorService::ExecutorService()
<<<<<<< HEAD
    : io_service_(), work_(new BackgroundWork(io_service_)), worker_([&]() { io_service_.run(); }) {}

ExecutorService::~ExecutorService() { close(); }
=======
    : io_service_(new boost::asio::io_service()),
      work_(new BackgroundWork(*io_service_)),
      worker_(std::bind(&ExecutorService::startWorker, this, io_service_)) {}

ExecutorService::~ExecutorService() {
    close();
    // If the worker_ is still not joinable at this point just detach
    // the thread so its destructor does not terminate the app
    if (worker_.joinable()) {
        worker_.detach();
    }
}

void ExecutorService::startWorker(std::shared_ptr<boost::asio::io_service> io_service) { io_service_->run(); }
>>>>>>> f773c602c... Test pr 10 (#27)

/*
 *  factory method of boost::asio::ip::tcp::socket associated with io_service_ instance
 *  @ returns shared_ptr to this socket
 */
SocketPtr ExecutorService::createSocket() {
<<<<<<< HEAD
    return std::make_shared<boost::asio::ip::tcp::socket>(std::ref(io_service_));
=======
    return SocketPtr(new boost::asio::ip::tcp::socket(*io_service_));
>>>>>>> f773c602c... Test pr 10 (#27)
}

TlsSocketPtr ExecutorService::createTlsSocket(SocketPtr &socket, boost::asio::ssl::context &ctx) {
    return std::shared_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket &> >(
        new boost::asio::ssl::stream<boost::asio::ip::tcp::socket &>(*socket, ctx));
}

/*
 *  factory method of Resolver object associated with io_service_ instance
 *  @returns shraed_ptr to resolver object
 */
TcpResolverPtr ExecutorService::createTcpResolver() {
<<<<<<< HEAD
    return std::make_shared<boost::asio::ip::tcp::resolver>(std::ref(io_service_));
}

DeadlineTimerPtr ExecutorService::createDeadlineTimer() {
    return std::make_shared<boost::asio::deadline_timer>(std::ref(io_service_));
}

void ExecutorService::close() {
    io_service_.stop();
    work_.reset();
    worker_.join();
}

void ExecutorService::postWork(std::function<void(void)> task) { io_service_.post(task); }
=======
    return TcpResolverPtr(new boost::asio::ip::tcp::resolver(*io_service_));
}

DeadlineTimerPtr ExecutorService::createDeadlineTimer() {
    return DeadlineTimerPtr(new boost::asio::deadline_timer(*io_service_));
}

void ExecutorService::close() {
    io_service_->stop();
    work_.reset();
    // If this thread is attempting to join itself, do not. The destructor's
    // call to close will handle joining if it does not occur here. This also ensures
    // join is not called twice since it is not re-entrant on windows
    if (std::this_thread::get_id() != worker_.get_id() && worker_.joinable()) {
        worker_.join();
    }
}

void ExecutorService::postWork(std::function<void(void)> task) { io_service_->post(task); }
>>>>>>> f773c602c... Test pr 10 (#27)

/////////////////////

ExecutorServiceProvider::ExecutorServiceProvider(int nthreads)
    : executors_(nthreads), executorIdx_(0), mutex_() {}

ExecutorServicePtr ExecutorServiceProvider::get() {
    Lock lock(mutex_);

    int idx = executorIdx_++ % executors_.size();
    if (!executors_[idx]) {
        executors_[idx] = std::make_shared<ExecutorService>();
    }

    return executors_[idx];
}

void ExecutorServiceProvider::close() {
    for (ExecutorList::iterator it = executors_.begin(); it != executors_.end(); ++it) {
        if (*it != NULL) {
            (*it)->close();
        }
        it->reset();
    }
}
}  // namespace pulsar
