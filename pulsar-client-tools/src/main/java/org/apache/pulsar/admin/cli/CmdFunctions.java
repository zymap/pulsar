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
package org.apache.pulsar.admin.cli;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
<<<<<<< HEAD
=======
import com.fasterxml.jackson.core.type.TypeReference;
>>>>>>> f773c602c... Test pr 10 (#27)
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
<<<<<<< HEAD
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.functions.FunctionState;
=======
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
>>>>>>> f773c602c... Test pr 10 (#27)

@Slf4j
@Parameters(commandDescription = "Interface for managing Pulsar Functions (lightweight, Lambda-style compute processes that work with Pulsar)")
public class CmdFunctions extends CmdBase {
    private final LocalRunner localRunner;
    private final CreateFunction creater;
    private final DeleteFunction deleter;
    private final UpdateFunction updater;
    private final GetFunction getter;
    private final GetFunctionStatus functionStatus;
    @Getter
    private final GetFunctionStats functionStats;
    private final RestartFunction restart;
    private final StopFunction stop;
    private final StartFunction start;
    private final ListFunctions lister;
    private final StateGetter stateGetter;
<<<<<<< HEAD
=======
    private final StatePutter statePutter;
>>>>>>> f773c602c... Test pr 10 (#27)
    private final TriggerFunction triggerer;
    private final UploadFunction uploader;
    private final DownloadFunction downloader;

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Override
        void run() throws Exception {
<<<<<<< HEAD
            processArguments();
=======
            try {
                processArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println();
                String chosenCommand = jcommander.getParsedCommand();
                usageFormatter.usage(chosenCommand);
                return;
            }
>>>>>>> f773c602c... Test pr 10 (#27)
            runCmd();
        }

        void processArguments() throws Exception {}

        abstract void runCmd() throws Exception;
    }

    /**
     * Namespace level command
     */
    @Getter
    abstract class NamespaceCommand extends BaseCommand {
<<<<<<< HEAD
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The function's namespace")
=======
        @Parameter(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The namespace of a Pulsar Function")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String namespace;

        @Override
        public void processArguments() {
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
        }
    }

    /**
     * Function level command
     */
    @Getter
    abstract class FunctionCommand extends BaseCommand {
        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;

<<<<<<< HEAD
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;

        @Parameter(names = "--name", description = "The function's name")
=======
        @Parameter(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The namespace of a Pulsar Function")
        protected String namespace;

        @Parameter(names = "--name", description = "The name of a Pulsar Function")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String functionName;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            boolean usesSetters = (null != tenant || null != namespace || null != functionName);
            boolean usesFqfn = (null != fqfn);

            // Throw an exception if --fqfn is set alongside any combination of --tenant, --namespace, and --name
            if (usesFqfn && usesSetters) {
                throw new RuntimeException(
                        "You must specify either a Fully Qualified Function Name (FQFN) or tenant, namespace, and function name");
            } else if (usesFqfn) {
                // If the --fqfn flag is used, parse tenant, namespace, and name using that flag
                String[] fqfnParts = fqfn.split("/");
                if (fqfnParts.length != 3) {
                    throw new RuntimeException(
                            "Fully qualified function names (FQFNs) must be of the form tenant/namespace/name");
                }
                tenant = fqfnParts[0];
                namespace = fqfnParts[1];
                functionName = fqfnParts[2];
            } else {
                if (tenant == null) {
                    tenant = PUBLIC_TENANT;
                }
                if (namespace == null) {
                    namespace = DEFAULT_NAMESPACE;
                }
                if (null == functionName) {
                    throw new RuntimeException(
                            "You must specify a name for the function or a Fully Qualified Function Name (FQFN)");
                }
            }
        }
    }

    /**
     * Commands that require a function config
     */
    @Getter
    abstract class FunctionDetailsCommand extends BaseCommand {
        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;
<<<<<<< HEAD
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The function's name")
        protected String functionName;
        // for backwards compatibility purposes
        @Parameter(names = "--className", description = "The function's class name", hidden = true)
        protected String DEPRECATED_className;
        @Parameter(names = "--classname", description = "The function's class name")
        protected String className;
        @Parameter(names = "--jar", description = "Path to the jar file for the function (if the function is written in Java). It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
=======
        @Parameter(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The namespace of a Pulsar Function")
        protected String namespace;
        @Parameter(names = "--name", description = "The name of a Pulsar Function")
        protected String functionName;
        // for backwards compatibility purposes
        @Parameter(names = "--className", description = "The class name of a Pulsar Function", hidden = true)
        protected String DEPRECATED_className;
        @Parameter(names = "--classname", description = "The class name of a Pulsar Function")
        protected String className;
        @Parameter(names = "--jar", description = "Path to the JAR file for the function (if the function is written in Java). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String jarFile;
        @Parameter(
                names = "--py",
                description = "Path to the main Python file/Python Wheel file for the function (if the function is written in Python)",
                listConverter = StringConverter.class)
        protected String pyFile;
<<<<<<< HEAD
        @Parameter(names = {"-i",
                "--inputs"}, description = "The function's input topic or topics (multiple topics can be specified as a comma-separated list)")
=======
        @Parameter(
                names = "--go",
                description = "Path to the main Go executable binary for the function (if the function is written in Go)")
        protected String goFile;
        @Parameter(names = {"-i",
                "--inputs"}, description = "The input topic or topics (multiple topics can be specified as a comma-separated list) of a Pulsar Function")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String inputs;
        // for backwards compatibility purposes
        @Parameter(names = "--topicsPattern", description = "TopicsPattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)", hidden = true)
        protected String DEPRECATED_topicsPattern;
        @Parameter(names = "--topics-pattern", description = "The topic pattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)")
        protected String topicsPattern;

<<<<<<< HEAD
        @Parameter(names = {"-o", "--output"}, description = "The function's output topic (If none is specified, no output is written)")
        protected String output;
        // for backwards compatibility purposes
        @Parameter(names = "--logTopic", description = "The topic to which the function's logs are produced", hidden = true)
        protected String DEPRECATED_logTopic;
        @Parameter(names = "--log-topic", description = "The topic to which the function's logs are produced")
=======
        @Parameter(names = {"-o", "--output"}, description = "The output topic of a Pulsar Function (If none is specified, no output is written)")
        protected String output;
        // for backwards compatibility purposes
        @Parameter(names = "--logTopic", description = "The topic to which the logs of a Pulsar Function are produced", hidden = true)
        protected String DEPRECATED_logTopic;
        @Parameter(names = "--log-topic", description = "The topic to which the logs of a Pulsar Function are produced")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String logTopic;

        @Parameter(names = {"-st", "--schema-type"}, description = "The builtin schema type or custom schema class name to be used for messages output by the function")
        protected String schemaType = "";

        // for backwards compatibility purposes
        @Parameter(names = "--customSerdeInputs", description = "The map of input topics to SerDe class names (as a JSON string)", hidden = true)
        protected String DEPRECATED_customSerdeInputString;
        @Parameter(names = "--custom-serde-inputs", description = "The map of input topics to SerDe class names (as a JSON string)")
        protected String customSerdeInputString;
<<<<<<< HEAD
        @Parameter(names = "--custom-schema-inputs", description = "The map of input topics to Schema class names (as a JSON string)")
        protected String customSchemaInputString;
=======
        @Parameter(names = "--custom-schema-inputs", description = "The map of input topics to Schema properties (as a JSON string)")
        protected String customSchemaInputString;
        @Parameter(names = "--custom-schema-outputs", description = "The map of input topics to Schema properties (as a JSON string)")
        protected String customSchemaOutputString;
        @Parameter(names = "--input-specs", description = "The map of inputs to custom configuration (as a JSON string)")
        protected String inputSpecs;
>>>>>>> f773c602c... Test pr 10 (#27)
        // for backwards compatibility purposes
        @Parameter(names = "--outputSerdeClassName", description = "The SerDe class to be used for messages output by the function", hidden = true)
        protected String DEPRECATED_outputSerdeClassName;
        @Parameter(names = "--output-serde-classname", description = "The SerDe class to be used for messages output by the function")
        protected String outputSerdeClassName;
        // for backwards compatibility purposes
<<<<<<< HEAD
        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file specifying the function's configuration", hidden = true)
        protected String DEPRECATED_fnConfigFile;
        @Parameter(names = "--function-config-file", description = "The path to a YAML config file specifying the function's configuration")
=======
        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file that specifies the configuration of a Pulsar Function", hidden = true)
        protected String DEPRECATED_fnConfigFile;
        @Parameter(names = "--function-config-file", description = "The path to a YAML config file that specifies the configuration of a Pulsar Function")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String fnConfigFile;
        // for backwards compatibility purposes
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the function", hidden = true)
        protected FunctionConfig.ProcessingGuarantees DEPRECATED_processingGuarantees;
        @Parameter(names = "--processing-guarantees", description = "The processing guarantees (aka delivery semantics) applied to the function")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        // for backwards compatibility purposes
        @Parameter(names = "--userConfig", description = "User-defined config key/values", hidden = true)
        protected String DEPRECATED_userConfigString;
        @Parameter(names = "--user-config", description = "User-defined config key/values")
        protected String userConfigString;
        @Parameter(names = "--retainOrdering", description = "Function consumes and processes messages in order", hidden = true)
        protected Boolean DEPRECATED_retainOrdering;
        @Parameter(names = "--retain-ordering", description = "Function consumes and processes messages in order")
<<<<<<< HEAD
        protected boolean retainOrdering;
        @Parameter(names = "--subs-name", description = "Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer")
        protected String subsName;
        @Parameter(names = "--parallelism", description = "The function's parallelism factor (i.e. the number of function instances to run)")
=======
        protected Boolean retainOrdering;
        @Parameter(names = "--forward-source-message-property", description = "Forwarding input message's properties to output topic when processing")
        protected Boolean forwardSourceMessageProperty = true;
        @Parameter(names = "--subs-name", description = "Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer")
        protected String subsName;
        @Parameter(names = "--subs-position", description = "Pulsar source subscription position if user wants to consume messages from the specified location")
        protected SubscriptionInitialPosition subsPosition;
        @Parameter(names = "--parallelism", description = "The parallelism factor of a Pulsar Function (i.e. the number of function instances to run)")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected Integer parallelism;
        @Parameter(names = "--cpu", description = "The cpu in cores that need to be allocated per function instance(applicable only to docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The ram in bytes that need to be allocated per function instance(applicable only to process/docker runtime)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk in bytes that need to be allocated per function instance(applicable only to docker runtime)")
        protected Long disk;
        // for backwards compatibility purposes
        @Parameter(names = "--windowLengthCount", description = "The number of messages per window", hidden = true)
        protected Integer DEPRECATED_windowLengthCount;
        @Parameter(names = "--window-length-count", description = "The number of messages per window")
        protected Integer windowLengthCount;
        // for backwards compatibility purposes
        @Parameter(names = "--windowLengthDurationMs", description = "The time duration of the window in milliseconds", hidden = true)
        protected Long DEPRECATED_windowLengthDurationMs;
        @Parameter(names = "--window-length-duration-ms", description = "The time duration of the window in milliseconds")
        protected Long windowLengthDurationMs;
        // for backwards compatibility purposes
        @Parameter(names = "--slidingIntervalCount", description = "The number of messages after which the window slides", hidden = true)
        protected Integer DEPRECATED_slidingIntervalCount;
        @Parameter(names = "--sliding-interval-count", description = "The number of messages after which the window slides")
        protected Integer slidingIntervalCount;
        // for backwards compatibility purposes
        @Parameter(names = "--slidingIntervalDurationMs", description = "The time duration after which the window slides", hidden = true)
        protected Long DEPRECATED_slidingIntervalDurationMs;
        @Parameter(names = "--sliding-interval-duration-ms", description = "The time duration after which the window slides")
        protected Long slidingIntervalDurationMs;
        // for backwards compatibility purposes
<<<<<<< HEAD
        @Parameter(names = "--autoAck", description = "Whether or not the framework will automatically acknowleges messages", hidden = true)
        protected Boolean DEPRECATED_autoAck = null;
        @Parameter(names = "--auto-ack", description = "Whether or not the framework will automatically acknowleges messages", arity = 1)
=======
        @Parameter(names = "--autoAck", description = "Whether or not the framework acknowledges messages automatically", hidden = true)
        protected Boolean DEPRECATED_autoAck = null;
        @Parameter(names = "--auto-ack", description = "Whether or not the framework acknowledges messages automatically", arity = 1)
>>>>>>> f773c602c... Test pr 10 (#27)
        protected Boolean autoAck;
        // for backwards compatibility purposes
        @Parameter(names = "--timeoutMs", description = "The message timeout in milliseconds", hidden = true)
        protected Long DEPRECATED_timeoutMs;
        @Parameter(names = "--timeout-ms", description = "The message timeout in milliseconds")
        protected Long timeoutMs;
        @Parameter(names = "--max-message-retries", description = "How many times should we try to process a message before giving up")
        protected Integer maxMessageRetries;
<<<<<<< HEAD
        @Parameter(names = "--dead-letter-topic", description = "The topic where all messages which could not be processed successfully are sent")
=======
        @Parameter(names = "--custom-runtime-options", description = "A string that encodes options to customize the runtime, see docs for configured runtime for details")
        protected String customRuntimeOptions;
        @Parameter(names = "--dead-letter-topic", description = "The topic where messages that are not processed successfully are sent to")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String deadLetterTopic;
        protected FunctionConfig functionConfig;
        protected String userCodeFile;

        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_className)) className = DEPRECATED_className;
            if (!StringUtils.isBlank(DEPRECATED_topicsPattern)) topicsPattern = DEPRECATED_topicsPattern;
            if (!StringUtils.isBlank(DEPRECATED_logTopic)) logTopic = DEPRECATED_logTopic;
            if (!StringUtils.isBlank(DEPRECATED_outputSerdeClassName)) outputSerdeClassName = DEPRECATED_outputSerdeClassName;
            if (!StringUtils.isBlank(DEPRECATED_customSerdeInputString)) customSerdeInputString = DEPRECATED_customSerdeInputString;

            if (!StringUtils.isBlank(DEPRECATED_fnConfigFile)) fnConfigFile = DEPRECATED_fnConfigFile;
            if (DEPRECATED_processingGuarantees != null) processingGuarantees = DEPRECATED_processingGuarantees;
            if (!StringUtils.isBlank(DEPRECATED_userConfigString)) userConfigString = DEPRECATED_userConfigString;
            if (DEPRECATED_retainOrdering != null) retainOrdering = DEPRECATED_retainOrdering;
            if (DEPRECATED_windowLengthCount != null) windowLengthCount = DEPRECATED_windowLengthCount;
            if (DEPRECATED_windowLengthDurationMs != null) windowLengthDurationMs = DEPRECATED_windowLengthDurationMs;
            if (DEPRECATED_slidingIntervalCount != null) slidingIntervalCount = DEPRECATED_slidingIntervalCount;
            if (DEPRECATED_slidingIntervalDurationMs != null) slidingIntervalDurationMs = DEPRECATED_slidingIntervalDurationMs;
            if (DEPRECATED_autoAck != null) autoAck = DEPRECATED_autoAck;
            if (DEPRECATED_timeoutMs != null) timeoutMs = DEPRECATED_timeoutMs;
        }

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            // merge deprecated args with new args
            mergeArgs();

            // Initialize config builder either from a supplied YAML config file or from scratch
            if (null != fnConfigFile) {
                functionConfig = CmdUtils.loadConfig(fnConfigFile, FunctionConfig.class);
            } else {
                functionConfig = new FunctionConfig();
            }

            if (null != fqfn) {
                parseFullyQualifiedFunctionName(fqfn, functionConfig);
            } else {
                if (null != tenant) {
                    functionConfig.setTenant(tenant);
                }
                if (null != namespace) {
                    functionConfig.setNamespace(namespace);
                }
                if (null != functionName) {
                    functionConfig.setName(functionName);
                }
            }

            if (null != inputs) {
                List<String> inputTopics = Arrays.asList(inputs.split(","));
                functionConfig.setInputs(inputTopics);
            }
            if (null != customSerdeInputString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, String> customSerdeInputMap = new Gson().fromJson(customSerdeInputString, type);
                functionConfig.setCustomSerdeInputs(customSerdeInputMap);
            }
            if (null != customSchemaInputString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, String> customschemaInputMap = new Gson().fromJson(customSchemaInputString, type);
                functionConfig.setCustomSchemaInputs(customschemaInputMap);
            }
<<<<<<< HEAD
=======
            if (null != customSchemaOutputString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, String> customSchemaOutputMap = new Gson().fromJson(customSchemaOutputString, type);
                functionConfig.setCustomSchemaOutputs(customSchemaOutputMap);
            }
            if (null != inputSpecs) {
                Type type = new TypeToken<Map<String, ConsumerConfig>>() {}.getType();
                functionConfig.setInputSpecs(new Gson().fromJson(inputSpecs, type));
            }
>>>>>>> f773c602c... Test pr 10 (#27)
            if (null != topicsPattern) {
                functionConfig.setTopicsPattern(topicsPattern);
            }
            if (null != output) {
                functionConfig.setOutput(output);
            }
            if (null != logTopic) {
                functionConfig.setLogTopic(logTopic);
            }
            if (null != className) {
                functionConfig.setClassName(className);
            }
            if (null != outputSerdeClassName) {
                functionConfig.setOutputSerdeClassName(outputSerdeClassName);
            }

            if (null != schemaType) {
                functionConfig.setOutputSchemaType(schemaType);
            }
            if (null != processingGuarantees) {
                functionConfig.setProcessingGuarantees(processingGuarantees);
            }

<<<<<<< HEAD
            functionConfig.setRetainOrdering(retainOrdering);
=======
            if (null != retainOrdering) {
                functionConfig.setRetainOrdering(retainOrdering);
            }

            if (null != forwardSourceMessageProperty) {
                functionConfig.setForwardSourceMessageProperty(forwardSourceMessageProperty);
            }
>>>>>>> f773c602c... Test pr 10 (#27)

            if (isNotBlank(subsName)) {
                functionConfig.setSubName(subsName);
            }

<<<<<<< HEAD
=======
            if (null != subsPosition) {
                functionConfig.setSubscriptionPosition(subsPosition);
            }

>>>>>>> f773c602c... Test pr 10 (#27)
            if (null != userConfigString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, Object> userConfigMap = new Gson().fromJson(userConfigString, type);
                functionConfig.setUserConfig(userConfigMap);
            }
            if (functionConfig.getUserConfig() == null) {
                functionConfig.setUserConfig(new HashMap<>());
            }

            if (parallelism != null) {
                functionConfig.setParallelism(parallelism);
            }

            Resources resources = functionConfig.getResources();
            if (cpu != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setCpu(cpu);
            }

            if (ram != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setRam(ram);
            }

            if (disk != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setDisk(disk);
            }
            if (resources != null) {
                functionConfig.setResources(resources);
            }

            if (timeoutMs != null) {
                functionConfig.setTimeoutMs(timeoutMs);
            }

<<<<<<< HEAD
=======
            if (customRuntimeOptions != null) {
                functionConfig.setCustomRuntimeOptions(customRuntimeOptions);
            }

>>>>>>> f773c602c... Test pr 10 (#27)
            // window configs
            WindowConfig windowConfig = functionConfig.getWindowConfig();
            if (null != windowLengthCount) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setWindowLengthCount(windowLengthCount);
            }
            if (null != windowLengthDurationMs) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setWindowLengthDurationMs(windowLengthDurationMs);
            }
            if (null != slidingIntervalCount) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setSlidingIntervalCount(slidingIntervalCount);
            }
            if (null != slidingIntervalDurationMs) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setSlidingIntervalDurationMs(slidingIntervalDurationMs);
            }

            functionConfig.setWindowConfig(windowConfig);

            if (autoAck != null) {
                functionConfig.setAutoAck(autoAck);
            }

            if (null != maxMessageRetries) {
                functionConfig.setMaxMessageRetries(maxMessageRetries);
            }
            if (null != deadLetterTopic) {
                functionConfig.setDeadLetterTopic(deadLetterTopic);
            }

            if (null != jarFile) {
                functionConfig.setJar(jarFile);
            }

            if (null != pyFile) {
                functionConfig.setPy(pyFile);
            }

<<<<<<< HEAD
=======
            if (null != goFile) {
                functionConfig.setGo(goFile);
            }

>>>>>>> f773c602c... Test pr 10 (#27)
            if (functionConfig.getJar() != null) {
                userCodeFile = functionConfig.getJar();
            } else if (functionConfig.getPy() != null) {
                userCodeFile = functionConfig.getPy();
<<<<<<< HEAD
=======
            } else if (functionConfig.getGo() != null) {
                userCodeFile = functionConfig.getGo();
>>>>>>> f773c602c... Test pr 10 (#27)
            }

            // check if configs are valid
            validateFunctionConfigs(functionConfig);
        }

        protected void validateFunctionConfigs(FunctionConfig functionConfig) {
<<<<<<< HEAD
            if (StringUtils.isEmpty(functionConfig.getClassName())) {
                throw new IllegalArgumentException("No Function Classname specified");
=======
            // go doesn't need className
            if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON || functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA){
                if (StringUtils.isEmpty(functionConfig.getClassName())) {
                    throw new IllegalArgumentException("No Function Classname specified");
                }
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            if (StringUtils.isEmpty(functionConfig.getName())) {
                org.apache.pulsar.common.functions.Utils.inferMissingFunctionName(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getTenant())) {
                org.apache.pulsar.common.functions.Utils.inferMissingTenant(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getNamespace())) {
                org.apache.pulsar.common.functions.Utils.inferMissingNamespace(functionConfig);
            }

<<<<<<< HEAD
            if (isNotBlank(functionConfig.getJar()) && isNotBlank(functionConfig.getPy())) {
                throw new ParameterException("Either a Java jar or a Python file needs to"
                        + " be specified for the function. Cannot specify both.");
            }

            if (isBlank(functionConfig.getJar()) && isBlank(functionConfig.getPy())) {
                throw new ParameterException("Either a Java jar or a Python file needs to"
=======
            if (isNotBlank(functionConfig.getJar()) && isNotBlank(functionConfig.getPy()) && isNotBlank(functionConfig.getGo())) {
                throw new ParameterException("Either a Java jar or a Python file or a Go executable binary needs to"
                        + " be specified for the function. Cannot specify both.");
            }

            if (isBlank(functionConfig.getJar()) && isBlank(functionConfig.getPy()) && isBlank(functionConfig.getGo())) {
                throw new ParameterException("Either a Java jar or a Python file or a Go executable binary needs to"
>>>>>>> f773c602c... Test pr 10 (#27)
                        + " be specified for the function. Please specify one.");
            }

            if (!isBlank(functionConfig.getJar()) && !Utils.isFunctionPackageUrlSupported(functionConfig.getJar()) &&
                    !new File(functionConfig.getJar()).exists()) {
                throw new ParameterException("The specified jar file does not exist");
            }
            if (!isBlank(functionConfig.getPy()) && !Utils.isFunctionPackageUrlSupported(functionConfig.getPy()) &&
                    !new File(functionConfig.getPy()).exists()) {
                throw new ParameterException("The specified python file does not exist");
            }
<<<<<<< HEAD
        }
    }

    @Parameters(commandDescription = "Run the Pulsar Function locally (rather than deploying it to the Pulsar cluster)")
    class LocalRunner extends FunctionDetailsCommand {

        // TODO: this should become bookkeeper url and it should be fetched from pulsar client.
        // for backwards compatibility purposes
        @Parameter(names = "--stateStorageServiceUrl", description = "The URL for the state storage service (by default Apache BookKeeper)", hidden = true)
        protected String DEPRECATED_stateStorageServiceUrl;
        @Parameter(names = "--state-storage-service-url", description = "The URL for the state storage service (by default Apache BookKeeper)")
        protected String stateStorageServiceUrl;
        // for backwards compatibility purposes
        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker", hidden = true)
        protected String DEPRECATED_brokerServiceUrl;
        @Parameter(names = "--broker-service-url", description = "The URL for the Pulsar broker")
=======
            if (!isBlank(functionConfig.getGo()) && !Utils.isFunctionPackageUrlSupported(functionConfig.getGo()) &&
                    !new File(functionConfig.getGo()).exists()) {
                throw new ParameterException("The specified go executable binary does not exist");
            }
        }
    }

    @Parameters(commandDescription = "Run a Pulsar Function locally, rather than deploy to a Pulsar cluster)")
    class LocalRunner extends FunctionDetailsCommand {

        // TODO: this should become BookKeeper URL and it should be fetched from Pulsar client.
        // for backwards compatibility purposes
        @Parameter(names = "--stateStorageServiceUrl", description = "The URL for the state storage service (the default is Apache BookKeeper)", hidden = true)
        protected String DEPRECATED_stateStorageServiceUrl;
        @Parameter(names = "--state-storage-service-url", description = "The URL for the state storage service (the default is Apache BookKeeper)")
        protected String stateStorageServiceUrl;
        // for backwards compatibility purposes
        @Parameter(names = "--brokerServiceUrl", description = "The URL for Pulsar broker", hidden = true)
        protected String DEPRECATED_brokerServiceUrl;
        @Parameter(names = "--broker-service-url", description = "The URL for Pulsar broker")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String brokerServiceUrl;
        // for backwards compatibility purposes
        @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using which function-process can connect to broker", hidden = true)
        protected String DEPRECATED_clientAuthPlugin;
        @Parameter(names = "--client-auth-plugin", description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;
        // for backwards compatibility purposes
        @Parameter(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
        protected String DEPRECATED_clientAuthParams;
        @Parameter(names = "--client-auth-params", description = "Client authentication param")
        protected String clientAuthParams;
        // for backwards compatibility purposes
        @Parameter(names = "--use_tls", description = "Use tls connection\n", hidden = true)
        protected Boolean DEPRECATED_useTls = null;
        @Parameter(names = "--use-tls", description = "Use tls connection\n")
        protected boolean useTls;
        // for backwards compatibility purposes
        @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection\n", hidden = true)
        protected Boolean DEPRECATED_tlsAllowInsecureConnection = null;
        @Parameter(names = "--tls-allow-insecure", description = "Allow insecure tls connection\n")
        protected boolean tlsAllowInsecureConnection;
        // for backwards compatibility purposes
        @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification", hidden = true)
        protected Boolean DEPRECATED_tlsHostNameVerificationEnabled = null;
        @Parameter(names = "--hostname-verification-enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;
        // for backwards compatibility purposes
        @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path", hidden = true)
        protected String DEPRECATED_tlsTrustCertFilePath;
        @Parameter(names = "--tls-trust-cert-path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;
        // for backwards compatibility purposes
        @Parameter(names = "--instanceIdOffset", description = "Start the instanceIds from this offset", hidden = true)
        protected Integer DEPRECATED_instanceIdOffset = null;
        @Parameter(names = "--instance-id-offset", description = "Start the instanceIds from this offset")
        protected Integer instanceIdOffset = 0;
<<<<<<< HEAD
=======
        @Parameter(names = "--runtime", description = "either THREAD or PROCESS. Only applies for Java functions")
        protected String runtime;
        @Parameter(names = "--secrets-provider-classname", description = "Whats the classname for secrets provider")
        protected String secretsProviderClassName;
        @Parameter(names = "--secrets-provider-config", description = "Config that needs to be passed to secrets provider")
        protected String secretsProviderConfig;
>>>>>>> f773c602c... Test pr 10 (#27)

        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_stateStorageServiceUrl)) stateStorageServiceUrl = DEPRECATED_stateStorageServiceUrl;
            if (!StringUtils.isBlank(DEPRECATED_brokerServiceUrl)) brokerServiceUrl = DEPRECATED_brokerServiceUrl;
            if (!StringUtils.isBlank(DEPRECATED_clientAuthPlugin)) clientAuthPlugin = DEPRECATED_clientAuthPlugin;
            if (!StringUtils.isBlank(DEPRECATED_clientAuthParams)) clientAuthParams = DEPRECATED_clientAuthParams;
            if (DEPRECATED_useTls != null) useTls = DEPRECATED_useTls;
            if (DEPRECATED_tlsAllowInsecureConnection != null) tlsAllowInsecureConnection = DEPRECATED_tlsAllowInsecureConnection;
            if (DEPRECATED_tlsHostNameVerificationEnabled != null) tlsHostNameVerificationEnabled = DEPRECATED_tlsHostNameVerificationEnabled;
            if (!StringUtils.isBlank(DEPRECATED_tlsTrustCertFilePath)) tlsTrustCertFilePath = DEPRECATED_tlsTrustCertFilePath;
            if (DEPRECATED_instanceIdOffset != null) instanceIdOffset = DEPRECATED_instanceIdOffset;
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            List<String> localRunArgs = new LinkedList<>();
            localRunArgs.add(System.getenv("PULSAR_HOME") + "/bin/function-localrunner");
            localRunArgs.add("--functionConfig");
            localRunArgs.add(new Gson().toJson(functionConfig));
            for (Field field : this.getClass().getDeclaredFields()) {
                if (field.getName().startsWith("DEPRECATED")) continue;
                if(field.getName().contains("$")) continue;
                Object value = field.get(this);
                if (value != null) {
                    localRunArgs.add("--" + field.getName());
                    localRunArgs.add(value.toString());
                }
            }
            ProcessBuilder processBuilder = new ProcessBuilder(localRunArgs).inheritIO();
            Process process = processBuilder.start();
            process.waitFor();
        }
    }

<<<<<<< HEAD
    @Parameters(commandDescription = "Create a Pulsar Function in cluster mode (i.e. deploy it on a Pulsar cluster)")
=======
    @Parameters(commandDescription = "Create a Pulsar Function in cluster mode (deploy it on a Pulsar cluster)")
>>>>>>> f773c602c... Test pr 10 (#27)
    class CreateFunction extends FunctionDetailsCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(functionConfig.getJar())) {
                admin.functions().createFunctionWithUrl(functionConfig, functionConfig.getJar());
            } else {
                admin.functions().createFunction(functionConfig, userCodeFile);
            }

            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Fetch information about a Pulsar Function")
    class GetFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            FunctionConfig functionConfig = admin.functions().getFunction(tenant, namespace, functionName);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(functionConfig));
        }
    }

    @Parameters(commandDescription = "Check the current status of a Pulsar Function")
    class GetFunctionStatus extends FunctionCommand {

<<<<<<< HEAD
        @Parameter(names = "--instance-id", description = "The function instanceId (Get-status of all instances if instance-id is not provided")
=======
        @Parameter(names = "--instance-id", description = "The function instanceId (Get-status of all instances if instance-id is not provided)")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isBlank(instanceId)) {
                print(admin.functions().getFunctionStatus(tenant, namespace, functionName));
            } else {
                print(admin.functions().getFunctionStatus(tenant, namespace, functionName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Parameters(commandDescription = "Get the current stats of a Pulsar Function")
    class GetFunctionStats extends FunctionCommand {

<<<<<<< HEAD
        @Parameter(names = "--instance-id", description = "The function instanceId (Get-status of all instances if instance-id is not provided")
=======
        @Parameter(names = "--instance-id", description = "The function instanceId (Get-stats of all instances if instance-id is not provided)")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String instanceId;

        @Override
        void runCmd() throws Exception {

            if (isBlank(instanceId)) {
                print(admin.functions().getFunctionStats(tenant, namespace, functionName));
            } else {
               print(admin.functions().getFunctionStats(tenant, namespace, functionName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Parameters(commandDescription = "Restart function instance")
    class RestartFunction extends FunctionCommand {

<<<<<<< HEAD
        @Parameter(names = "--instance-id", description = "The function instanceId (restart all instances if instance-id is not provided")
=======
        @Parameter(names = "--instance-id", description = "The function instanceId (restart all instances if instance-id is not provided)")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    admin.functions().restartFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                admin.functions().restartFunction(tenant, namespace, functionName);
            }
            System.out.println("Restarted successfully");
        }
    }

    @Parameters(commandDescription = "Stops function instance")
    class StopFunction extends FunctionCommand {

<<<<<<< HEAD
        @Parameter(names = "--instance-id", description = "The function instanceId (stop all instances if instance-id is not provided")
=======
        @Parameter(names = "--instance-id", description = "The function instanceId (stop all instances if instance-id is not provided)")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    admin.functions().stopFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                admin.functions().stopFunction(tenant, namespace, functionName);
            }
            System.out.println("Stopped successfully");
        }
    }

    @Parameters(commandDescription = "Starts a stopped function instance")
    class StartFunction extends FunctionCommand {

<<<<<<< HEAD
        @Parameter(names = "--instance-id", description = "The function instanceId (start all instances if instance-id is not provided")
=======
        @Parameter(names = "--instance-id", description = "The function instanceId (start all instances if instance-id is not provided)")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    admin.functions().startFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                admin.functions().startFunction(tenant, namespace, functionName);
            }
            System.out.println("Started successfully");
        }
    }

<<<<<<< HEAD
    @Parameters(commandDescription = "Delete a Pulsar Function that's running on a Pulsar cluster")
=======
    @Parameters(commandDescription = "Delete a Pulsar Function that is running on a Pulsar cluster")
>>>>>>> f773c602c... Test pr 10 (#27)
    class DeleteFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            admin.functions().deleteFunction(tenant, namespace, functionName);
            print("Deleted successfully");
        }
    }

<<<<<<< HEAD
    @Parameters(commandDescription = "Update a Pulsar Function that's been deployed to a Pulsar cluster")
    class UpdateFunction extends FunctionDetailsCommand {

=======
    @Parameters(commandDescription = "Update a Pulsar Function that has been deployed to a Pulsar cluster")
    class UpdateFunction extends FunctionDetailsCommand {

        @Parameter(names = "--update-auth-data", description = "Whether or not to update the auth data")
        protected boolean updateAuthData;

>>>>>>> f773c602c... Test pr 10 (#27)
        @Override
        protected void validateFunctionConfigs(FunctionConfig functionConfig) {
            if (StringUtils.isEmpty(functionConfig.getClassName())) {
                if (StringUtils.isEmpty(functionConfig.getName())) {
                    throw new IllegalArgumentException("Function Name not provided");
                }
            } else if (StringUtils.isEmpty(functionConfig.getName())) {
                org.apache.pulsar.common.functions.Utils.inferMissingFunctionName(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getTenant())) {
                org.apache.pulsar.common.functions.Utils.inferMissingTenant(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getNamespace())) {
                org.apache.pulsar.common.functions.Utils.inferMissingNamespace(functionConfig);
            }
        }

        @Override
        void runCmd() throws Exception {
<<<<<<< HEAD
            if (Utils.isFunctionPackageUrlSupported(functionConfig.getJar())) {
                admin.functions().updateFunctionWithUrl(functionConfig, functionConfig.getJar());
            } else {
                admin.functions().updateFunction(functionConfig, userCodeFile);
=======

            UpdateOptions updateOptions = new UpdateOptions();
            updateOptions.setUpdateAuthData(updateAuthData);
            if (Utils.isFunctionPackageUrlSupported(functionConfig.getJar())) {
                admin.functions().updateFunctionWithUrl(functionConfig, functionConfig.getJar(), updateOptions);
            } else {
                admin.functions().updateFunction(functionConfig, userCodeFile, updateOptions);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            print("Updated successfully");
        }
    }

<<<<<<< HEAD
    @Parameters(commandDescription = "List all of the Pulsar Functions running under a specific tenant and namespace")
=======
    @Parameters(commandDescription = "List all Pulsar Functions running under a specific tenant and namespace")
>>>>>>> f773c602c... Test pr 10 (#27)
    class ListFunctions extends NamespaceCommand {
        @Override
        void runCmd() throws Exception {
            print(admin.functions().getFunctions(tenant, namespace));
        }
    }

<<<<<<< HEAD
    @Parameters(commandDescription = "Fetch the current state associated with a Pulsar Function running in cluster mode")
    class StateGetter extends FunctionCommand {

        @Parameter(names = { "-k", "--key" }, description = "key")
=======
    @Parameters(commandDescription = "Fetch the current state associated with a Pulsar Function")
    class StateGetter extends FunctionCommand {

        @Parameter(names = { "-k", "--key" }, description = "Key name of State")
>>>>>>> f773c602c... Test pr 10 (#27)
        private String key = null;

        @Parameter(names = { "-w", "--watch" }, description = "Watch for changes in the value associated with a key for a Pulsar Function")
        private boolean watch = false;

        @Override
        void runCmd() throws Exception {
<<<<<<< HEAD
            do {
                FunctionState functionState = admin.functions().getFunctionState(tenant, namespace, functionName, key);
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                System.out.println(gson.toJson(functionState));
=======
            if (isBlank(key)) {
                throw new ParameterException("State key needs to be specified");
            }
            do {
                try {
                    FunctionState functionState = admin.functions()
                                                       .getFunctionState(tenant, namespace, functionName, key);
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    System.out.println(gson.toJson(functionState));
                } catch (PulsarAdminException pae) {
                    if (pae.getStatusCode() == 404 && watch) {
                        System.err.println(pae.getMessage());
                    } else {
                        throw pae;
                    }
                }
>>>>>>> f773c602c... Test pr 10 (#27)
                if (watch) {
                    Thread.sleep(1000);
                }
            } while (watch);
        }
    }

<<<<<<< HEAD
    @Parameters(commandDescription = "Triggers the specified Pulsar Function with a supplied value")
=======
    @Parameters(commandDescription = "Put the state associated with a Pulsar Function")
    class StatePutter extends FunctionCommand {

        @Parameter(names = { "-s", "--state" }, description = "The FunctionState that needs to be put", required = true)
        private String state = null;

        @Override
        void runCmd() throws Exception {
            TypeReference<FunctionState> typeRef
                    = new TypeReference<FunctionState>() {};
            FunctionState stateRepr = ObjectMapperFactory.getThreadLocal().readValue(state, typeRef);
            admin.functions()
                    .putFunctionState(tenant, namespace, functionName, stateRepr);
        }
    }

    @Parameters(commandDescription = "Trigger the specified Pulsar Function with a supplied value")
>>>>>>> f773c602c... Test pr 10 (#27)
    class TriggerFunction extends FunctionCommand {
        // for backward compatibility purposes
        @Parameter(names = "--triggerValue", description = "The value with which you want to trigger the function", hidden = true)
        protected String DEPRECATED_triggerValue;
        @Parameter(names = "--trigger-value", description = "The value with which you want to trigger the function")
        protected String triggerValue;
        // for backward compatibility purposes
<<<<<<< HEAD
        @Parameter(names = "--triggerFile", description = "The path to the file that contains the data with which you'd like to trigger the function", hidden = true)
        protected String DEPRECATED_triggerFile;
        @Parameter(names = "--trigger-file", description = "The path to the file that contains the data with which you'd like to trigger the function")
=======
        @Parameter(names = "--triggerFile", description = "The path to the file that contains the data with which you want to trigger the function", hidden = true)
        protected String DEPRECATED_triggerFile;
        @Parameter(names = "--trigger-file", description = "The path to the file that contains the data with which you want to trigger the function")
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String triggerFile;
        @Parameter(names = "--topic", description = "The specific topic name that the function consumes from that you want to inject the data to")
        protected String topic;

        public void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_triggerValue)) triggerValue = DEPRECATED_triggerValue;
            if (!StringUtils.isBlank(DEPRECATED_triggerFile)) triggerFile = DEPRECATED_triggerFile;
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            if (triggerFile == null && triggerValue == null) {
                throw new ParameterException("Either a trigger value or a trigger filepath needs to be specified");
            }
            String retval = admin.functions().triggerFunction(tenant, namespace, functionName, topic, triggerValue, triggerFile);
            System.out.println(retval);
        }
    }

    @Parameters(commandDescription = "Upload File Data to Pulsar", hidden = true)
    class UploadFunction extends BaseCommand {
        // for backward compatibility purposes
        @Parameter(
                names = "--sourceFile",
                description = "The file whose contents need to be uploaded",
                listConverter = StringConverter.class, hidden = true)
        protected String DEPRECATED_sourceFile;
        @Parameter(
                names = "--source-file",
                description = "The file whose contents need to be uploaded",
                listConverter = StringConverter.class)
        protected String sourceFile;
        @Parameter(
                names = "--path",
                description = "Path where the contents need to be stored",
                listConverter = StringConverter.class, required = true)
        protected String path;

        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_sourceFile)) sourceFile = DEPRECATED_sourceFile;
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            if (StringUtils.isBlank(sourceFile)) {
                throw new ParameterException("--source-file needs to be specified");
            }
            admin.functions().uploadFunction(sourceFile, path);
            print("Uploaded successfully");
        }
    }

    @Parameters(commandDescription = "Download File Data from Pulsar", hidden = true)
<<<<<<< HEAD
    class DownloadFunction extends BaseCommand {
        // for backward compatibility purposes
        @Parameter(
                names = "--destinationFile",
                description = "The file where downloaded contents need to be stored",
=======
    class DownloadFunction extends FunctionCommand {
        // for backward compatibility purposes
        @Parameter(
                names = "--destinationFile",
                description = "The file to store downloaded content",
>>>>>>> f773c602c... Test pr 10 (#27)
                listConverter = StringConverter.class, hidden = true)
        protected String DEPRECATED_destinationFile;
        @Parameter(
                names = "--destination-file",
<<<<<<< HEAD
                description = "The file where downloaded contents need to be stored",
=======
                description = "The file to store downloaded content",
>>>>>>> f773c602c... Test pr 10 (#27)
                listConverter = StringConverter.class)
        protected String destinationFile;
        @Parameter(
                names = "--path",
<<<<<<< HEAD
                description = "Path where the contents are to be stored",
                listConverter = StringConverter.class, required = true)
=======
                description = "Path to store the content",
                listConverter = StringConverter.class, required = false, hidden = true)
>>>>>>> f773c602c... Test pr 10 (#27)
        protected String path;

        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_destinationFile)) destinationFile = DEPRECATED_destinationFile;
        }

        @Override
<<<<<<< HEAD
=======
        void processArguments() throws Exception {
            if (path == null) {
                super.processArguments();
            }
        }

        @Override
>>>>>>> f773c602c... Test pr 10 (#27)
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            if (StringUtils.isBlank(destinationFile)) {
                throw new ParameterException("--destination-file needs to be specified");
            }
<<<<<<< HEAD
            admin.functions().downloadFunction(destinationFile, path);
=======
            if (path != null) {
                admin.functions().downloadFunction(destinationFile, path);
            } else {
                admin.functions().downloadFunction(destinationFile, tenant, namespace, functionName);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
            print("Downloaded successfully");
        }
    }

    public CmdFunctions(PulsarAdmin admin) throws PulsarClientException {
        super("functions", admin);
        localRunner = new LocalRunner();
        creater = new CreateFunction();
        deleter = new DeleteFunction();
        updater = new UpdateFunction();
        getter = new GetFunction();
        functionStatus = new GetFunctionStatus();
        functionStats = new GetFunctionStats();
        lister = new ListFunctions();
        stateGetter = new StateGetter();
<<<<<<< HEAD
=======
        statePutter = new StatePutter();
>>>>>>> f773c602c... Test pr 10 (#27)
        triggerer = new TriggerFunction();
        uploader = new UploadFunction();
        downloader = new DownloadFunction();
        restart = new RestartFunction();
        stop = new StopFunction();
        start = new StartFunction();
        jcommander.addCommand("localrun", getLocalRunner());
        jcommander.addCommand("create", getCreater());
        jcommander.addCommand("delete", getDeleter());
        jcommander.addCommand("update", getUpdater());
        jcommander.addCommand("get", getGetter());
        jcommander.addCommand("restart", getRestarter());
        jcommander.addCommand("stop", getStopper());
        jcommander.addCommand("start", getStarter());
        // TODO depecreate getstatus
        jcommander.addCommand("status", getStatuser(), "getstatus");
        jcommander.addCommand("stats", getFunctionStats());
        jcommander.addCommand("list", getLister());
        jcommander.addCommand("querystate", getStateGetter());
<<<<<<< HEAD
=======
        jcommander.addCommand("putstate", getStatePutter());
>>>>>>> f773c602c... Test pr 10 (#27)
        jcommander.addCommand("trigger", getTriggerer());
        jcommander.addCommand("upload", getUploader());
        jcommander.addCommand("download", getDownloader());
    }

    @VisibleForTesting
    LocalRunner getLocalRunner() {
        return localRunner;
    }

    @VisibleForTesting
    CreateFunction getCreater() {
        return creater;
    }

    @VisibleForTesting
    DeleteFunction getDeleter() {
        return deleter;
    }

    @VisibleForTesting
    UpdateFunction getUpdater() {
        return updater;
    }

    @VisibleForTesting
    GetFunction getGetter() {
        return getter;
    }

    @VisibleForTesting
    GetFunctionStatus getStatuser() { return functionStatus; }

    @VisibleForTesting
    ListFunctions getLister() {
        return lister;
    }

    @VisibleForTesting
<<<<<<< HEAD
=======
    StatePutter getStatePutter() {
        return statePutter;
    }

    @VisibleForTesting
>>>>>>> f773c602c... Test pr 10 (#27)
    StateGetter getStateGetter() {
        return stateGetter;
    }

    @VisibleForTesting
    TriggerFunction getTriggerer() {
        return triggerer;
    }

    @VisibleForTesting
    UploadFunction getUploader() {
        return uploader;
    }

    @VisibleForTesting
    DownloadFunction getDownloader() {
        return downloader;
    }

    @VisibleForTesting
    RestartFunction getRestarter() {
        return restart;
    }

    @VisibleForTesting
    StopFunction getStopper() {
        return stop;
    }

    @VisibleForTesting
    StartFunction getStarter() {
        return start;
    }

    private void parseFullyQualifiedFunctionName(String fqfn, FunctionConfig functionConfig) {
        String[] args = fqfn.split("/");
        if (args.length != 3) {
            throw new ParameterException("Fully qualified function names (FQFNs) must be of the form tenant/namespace/name");
        } else {
            functionConfig.setTenant(args[0]);
            functionConfig.setNamespace(args[1]);
            functionConfig.setName(args[2]);
        }
    }

}
