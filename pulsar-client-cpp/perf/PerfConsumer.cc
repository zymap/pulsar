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
#include <lib/LogUtils.h>
DECLARE_LOG_OBJECT()

#include <chrono>
#include <thread>
#include <iostream>
#include <fstream>
#include <mutex>
#include <functional>

using namespace std::chrono;

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/program_options.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/p_square_quantile.hpp>
namespace po = boost::program_options;
using namespace boost::accumulators;

#include <lib/Latch.h>

#include <pulsar/Client.h>
#include <pulsar/Authentication.h>
using namespace pulsar;

static int64_t currentTimeMillis() {
    using namespace boost::posix_time;
    using boost::posix_time::milliseconds;
    using boost::posix_time::seconds;
    static ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));

    time_duration diff = microsec_clock::universal_time() - time_t_epoch;
    return diff.total_milliseconds();
}

struct Arguments {
    std::string authParams;
    std::string authPlugin;
    bool isUseTls;
    bool isTlsAllowInsecureConnection;
    std::string tlsTrustCertsFilePath;
    std::string topic;
    int numTopics;
    int numConsumers;
    std::string subscriberName;
    int waitTimeMs;
    std::string serviceURL;
    int receiverQueueSize;
    int ioThreads;
    int listenerThreads;
    std::string encKeyName;
    std::string encKeyValueFile;
};

namespace pulsar {
class PulsarFriend {
<<<<<<< HEAD
 public:
    static Client getClient(const std::string& url, const ClientConfiguration conf,
                            bool poolConnections) {
        return Client(url, conf, poolConnections);
    }
};
}
=======
   public:
    static Client getClient(const std::string& url, const ClientConfiguration conf, bool poolConnections) {
        return Client(url, conf, poolConnections);
    }
};
}  // namespace pulsar
>>>>>>> f773c602c... Test pr 10 (#27)

#if __GNUC__ == 4 && __GNUC_MINOR__ == 4
// Used for gcc-4.4.7 with boost-1.41
#include <cstdatomic>
#else
#include <atomic>
#endif

<<<<<<< HEAD
class EncKeyReader: public CryptoKeyReader {

  private:
=======
class EncKeyReader : public CryptoKeyReader {
   private:
>>>>>>> f773c602c... Test pr 10 (#27)
    std::string privKeyContents;

    void readFile(std::string fileName, std::string& fileContents) const {
        std::ifstream ifs(fileName);
        std::stringstream fileStream;
        fileStream << ifs.rdbuf();
        fileContents = fileStream.str();
    }

<<<<<<< HEAD
  public:

=======
   public:
>>>>>>> f773c602c... Test pr 10 (#27)
    EncKeyReader(std::string keyFile) {
        if (keyFile.empty()) {
            return;
        }
        readFile(keyFile, privKeyContents);
    }

<<<<<<< HEAD
    Result getPublicKey(const std::string &keyName, std::map<std::string, std::string>& metadata, EncryptionKeyInfo& encKeyInfo) const {
        return ResultInvalidConfiguration;
    }

    Result getPrivateKey(const std::string &keyName, std::map<std::string, std::string>& metadata, EncryptionKeyInfo& encKeyInfo) const {
=======
    Result getPublicKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                        EncryptionKeyInfo& encKeyInfo) const {
        return ResultInvalidConfiguration;
    }

    Result getPrivateKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                         EncryptionKeyInfo& encKeyInfo) const {
>>>>>>> f773c602c... Test pr 10 (#27)
        encKeyInfo.setKey(privKeyContents);
        return ResultOk;
    }
};

// Counters
std::atomic<uint32_t> messagesReceived;
std::atomic<uint32_t> bytesReceived;

typedef std::chrono::high_resolution_clock Clock;

<<<<<<< HEAD
void handleAckComplete(Result) {
}

=======
void handleAckComplete(Result) {}
>>>>>>> f773c602c... Test pr 10 (#27)

std::mutex mutex;
typedef std::unique_lock<std::mutex> Lock;
typedef accumulator_set<uint64_t, stats<tag::mean, tag::p_square_quantile> > LatencyAccumulator;
LatencyAccumulator e2eLatencyAccumulator(quantile_probability = 0.99);

void messageListener(Consumer consumer, const Message& msg) {
    ++messagesReceived;
    bytesReceived += msg.getLength();

    int64_t e2eLatencyMsec = currentTimeMillis() - msg.getPublishTimestamp();
    Lock lock(mutex);
    e2eLatencyAccumulator(e2eLatencyMsec);
    lock.unlock();

    consumer.acknowledgeAsync(msg, handleAckComplete);
}

std::vector<Consumer> consumers;

void handleSubscribe(Result result, Consumer consumer, Latch latch) {
    if (result != ResultOk) {
<<<<<<< HEAD
        LOG_ERROR("Error creating consumer: "<< result);
=======
        LOG_ERROR("Error creating consumer: " << result);
>>>>>>> f773c602c... Test pr 10 (#27)
        exit(-1);
    }

    Lock lock(mutex);
    consumers.push_back(consumer);

    latch.countdown();
}

void startPerfConsumer(const Arguments& args) {
    ClientConfiguration conf;

    conf.setUseTls(args.isUseTls);
    conf.setTlsAllowInsecureConnection(args.isTlsAllowInsecureConnection);
    if (!args.tlsTrustCertsFilePath.empty()) {
        std::string tlsTrustCertsFilePath(args.tlsTrustCertsFilePath);
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
    }
    conf.setIOThreads(args.ioThreads);
    conf.setMessageListenerThreads(args.listenerThreads);
<<<<<<< HEAD
    if(!args.authPlugin.empty()) {
=======
    if (!args.authPlugin.empty()) {
>>>>>>> f773c602c... Test pr 10 (#27)
        AuthenticationPtr auth = AuthFactory::create(args.authPlugin, args.authParams);
        conf.setAuth(auth);
    }

    Client client(pulsar::PulsarFriend::getClient(args.serviceURL, conf, false));

    ConsumerConfiguration consumerConf;
    consumerConf.setMessageListener(messageListener);
    consumerConf.setReceiverQueueSize(args.receiverQueueSize);
    std::shared_ptr<EncKeyReader> keyReader = std::make_shared<EncKeyReader>(args.encKeyValueFile);
    if (!args.encKeyName.empty()) {
        consumerConf.setCryptoKeyReader(keyReader);
    }

    Latch latch(args.numTopics * args.numConsumers);

    for (int i = 0; i < args.numTopics; i++) {
<<<<<<< HEAD
        std::string topic =
                (args.numTopics == 1) ?
                        args.topic : args.topic + "-" + std::to_string(i);
=======
        std::string topic = (args.numTopics == 1) ? args.topic : args.topic + "-" + std::to_string(i);
>>>>>>> f773c602c... Test pr 10 (#27)
        LOG_INFO("Adding " << args.numConsumers << " consumers on topic " << topic);

        for (int j = 0; j < args.numConsumers; j++) {
            std::string subscriberName;
            if (args.numConsumers > 1) {
                subscriberName = args.subscriberName + "-" + std::to_string(j);
            } else {
                subscriberName = args.subscriberName;
            }

<<<<<<< HEAD
            client.subscribeAsync(topic, subscriberName, consumerConf,
                                  std::bind(handleSubscribe, std::placeholders::_1, std::placeholders::_2, latch));
=======
            client.subscribeAsync(
                topic, subscriberName, consumerConf,
                std::bind(handleSubscribe, std::placeholders::_1, std::placeholders::_2, latch));
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    Clock::time_point oldTime = Clock::now();

    latch.wait();
<<<<<<< HEAD
    LOG_INFO(
            "Start receiving from " << args.numConsumers << " consumers on " << args.numTopics << " topics");
=======
    LOG_INFO("Start receiving from " << args.numConsumers << " consumers on " << args.numTopics << " topics");
>>>>>>> f773c602c... Test pr 10 (#27)

    while (true) {
        std::this_thread::sleep_for(seconds(10));

        Clock::time_point now = Clock::now();
<<<<<<< HEAD
        double elapsed = duration_cast < milliseconds > (now - oldTime).count() / 1e3;
=======
        double elapsed = duration_cast<milliseconds>(now - oldTime).count() / 1e3;
>>>>>>> f773c602c... Test pr 10 (#27)

        double rate = messagesReceived.exchange(0) / elapsed;
        double throughput = bytesReceived.exchange(0) / elapsed / 1024 / 1024 * 8;

        Lock lock(mutex);
        int64_t e2eLatencyAvgMs = rate > 0.0 ? mean(e2eLatencyAccumulator) : 0;
        int64_t e2eLatency99pctMs = p_square_quantile(e2eLatencyAccumulator);
        e2eLatencyAccumulator = LatencyAccumulator(quantile_probability = 0.99);
        lock.unlock();

        LOG_INFO("Throughput received: " << rate << "  msg/s --- " << throughput << " Mbit/s ---"  //
<<<<<<< HEAD
                << " End-To-End latency: avg: " << e2eLatencyAvgMs << " ms -- 99pct: " << e2eLatency99pctMs << " ms");
=======
                                         << " End-To-End latency: avg: " << e2eLatencyAvgMs
                                         << " ms -- 99pct: " << e2eLatency99pctMs << " ms");
>>>>>>> f773c602c... Test pr 10 (#27)

        oldTime = now;
    }
}

int main(int argc, char** argv) {
    // First try to read default values from config file if present
    const std::string confFile = "conf/client.conf";
    std::string defaultServiceUrl;

    std::ifstream file(confFile.c_str());
    if (file) {
        po::variables_map vm;
        po::options_description confFileDesc;
        confFileDesc.add_options()  //
<<<<<<< HEAD
        ("serviceURL", po::value<std::string>()->default_value("pulsar://localhost:6650"));
=======
            ("serviceURL", po::value<std::string>()->default_value("pulsar://localhost:6650"));
>>>>>>> f773c602c... Test pr 10 (#27)

        po::store(po::parse_config_file<char>(file, confFileDesc, true), vm);
        po::notify(vm);

        defaultServiceUrl = vm["serviceURL"].as<std::string>();
    }

    Arguments args;

    // Declare the supported options.
    po::positional_options_description positional;
    positional.add("topic", 1);

    po::options_description desc("Allowed options");
    desc.add_options()  //

<<<<<<< HEAD
    ("help,h", "Print this help message")  //

    ("auth-params,v", po::value<std::string>(&args.authParams)->default_value(""), "Authentication parameters, e.g., \"key1:val1,key2:val2\"") //

    ("auth-plugin,a", po::value<std::string>(&args.authPlugin)->default_value(""), "Authentication plugin class library path") //

    ("use-tls,b", po::value<bool>(&args.isUseTls)->default_value(false), "Whether tls connection is used")  //

    ("allow-insecure,d", po::value<bool>(&args.isTlsAllowInsecureConnection)->default_value(true), "Whether insecure tls connection is allowed")  //

    ("trust-cert-file,c", po::value<std::string>(&args.tlsTrustCertsFilePath)->default_value(""), "TLS trust certification file path")  //

    ("num-topics,t", po::value<int>(&args.numTopics)->default_value(1), "Number of topics")  //

    ("num-consumers,n", po::value<int>(&args.numConsumers)->default_value(1),
     "Number of consumers (per topic)")  //

    ("subscriber-name,s", po::value<std::string>(&args.subscriberName)->default_value("sub"),
     "Subscriber name prefix")  //

    ("wait-time,w", po::value<int>(&args.waitTimeMs)->default_value(1),
     "Simulate a slow message consumer (Delay in ms)")  //

    ("service-url,u", po::value<std::string>(&args.serviceURL)->default_value(defaultServiceUrl),
     "Pulsar Service URL")  //

    ("receiver-queue-size,p", po::value<int>(&args.receiverQueueSize)->default_value(1000),
     "Size of the receiver queue")  //

    ("io-threads,i", po::value<int>(&args.ioThreads)->default_value(1),
     "Number of IO threads to use")  //

    ("listener-threads,l", po::value<int>(&args.listenerThreads)->default_value(1),
     "Number of listener threads") //

    ("encryption-key-name,k", po::value<std::string>(&args.encKeyName)->default_value(""), "The private key name to decrypt payload") //

    ("encryption-key-value-file,f", po::value<std::string>(&args.encKeyValueFile)->default_value(""),
            "The file which contains the private key to decrypt payload"); //
=======
        ("help,h", "Print this help message")  //

        ("auth-params,v", po::value<std::string>(&args.authParams)->default_value(""),
         "Authentication parameters, e.g., \"key1:val1,key2:val2\"")  //

        ("auth-plugin,a", po::value<std::string>(&args.authPlugin)->default_value(""),
         "Authentication plugin class library path")  //

        ("use-tls,b", po::value<bool>(&args.isUseTls)->default_value(false),
         "Whether tls connection is used")  //

        ("allow-insecure,d", po::value<bool>(&args.isTlsAllowInsecureConnection)->default_value(true),
         "Whether insecure tls connection is allowed")  //

        ("trust-cert-file,c", po::value<std::string>(&args.tlsTrustCertsFilePath)->default_value(""),
         "TLS trust certification file path")  //

        ("num-topics,t", po::value<int>(&args.numTopics)->default_value(1), "Number of topics")  //

        ("num-consumers,n", po::value<int>(&args.numConsumers)->default_value(1),
         "Number of consumers (per topic)")  //

        ("subscriber-name,s", po::value<std::string>(&args.subscriberName)->default_value("sub"),
         "Subscriber name prefix")  //

        ("wait-time,w", po::value<int>(&args.waitTimeMs)->default_value(1),
         "Simulate a slow message consumer (Delay in ms)")  //

        ("service-url,u", po::value<std::string>(&args.serviceURL)->default_value(defaultServiceUrl),
         "Pulsar Service URL")  //

        ("receiver-queue-size,p", po::value<int>(&args.receiverQueueSize)->default_value(1000),
         "Size of the receiver queue")  //

        ("io-threads,i", po::value<int>(&args.ioThreads)->default_value(1),
         "Number of IO threads to use")  //

        ("listener-threads,l", po::value<int>(&args.listenerThreads)->default_value(1),
         "Number of listener threads")  //

        ("encryption-key-name,k", po::value<std::string>(&args.encKeyName)->default_value(""),
         "The private key name to decrypt payload")  //

        ("encryption-key-value-file,f", po::value<std::string>(&args.encKeyValueFile)->default_value(""),
         "The file which contains the private key to decrypt payload");  //
>>>>>>> f773c602c... Test pr 10 (#27)

    po::options_description hidden;
    hidden.add_options()("topic", po::value<std::string>(&args.topic), "Topic name");

    po::options_description allOptions;
    allOptions.add(desc).add(hidden);

    po::variables_map map;
    try {
<<<<<<< HEAD
        po::store(
                po::command_line_parser(argc, argv).options(allOptions).positional(positional).run(),
                map);
=======
        po::store(po::command_line_parser(argc, argv).options(allOptions).positional(positional).run(), map);
>>>>>>> f773c602c... Test pr 10 (#27)
        po::notify(map);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing parameters -- " << e.what() << std::endl << std::endl;
        std::cerr << desc << std::endl;
        return -1;
    }

    if (map.count("help")) {
        std::cerr << desc << std::endl;
        return -1;
    }

    if (map.count("topic") != 1) {
<<<<<<< HEAD
        std::cerr << "Need to specify a topic name. eg: persistent://prop/cluster/ns/my-topic"
                << std::endl << std::endl;
=======
        std::cerr << "Need to specify a topic name. eg: persistent://prop/cluster/ns/my-topic" << std::endl
                  << std::endl;
>>>>>>> f773c602c... Test pr 10 (#27)
        std::cerr << desc << std::endl;
        return -1;
    }

    LOG_INFO("--- Consumer configuration ---");
    for (po::variables_map::iterator it = map.begin(); it != map.end(); ++it) {
        if (it->second.value().type() == typeid(std::string)) {
            LOG_INFO(it->first << ": " << it->second.as<std::string>());
        } else if (it->second.value().type() == typeid(int)) {
            LOG_INFO(it->first << ": " << it->second.as<int>());
        } else if (it->second.value().type() == typeid(double)) {
            LOG_INFO(it->first << ": " << it->second.as<double>());
        }
    }

    LOG_INFO("------------------------------");

    startPerfConsumer(args);
}
