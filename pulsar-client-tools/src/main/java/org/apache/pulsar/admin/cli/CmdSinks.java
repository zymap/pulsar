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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
<<<<<<< HEAD
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
=======
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptions;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.functions.Utils;

@Getter
@Parameters(commandDescription = "Interface for managing Pulsar IO sinks (egress data from Pulsar)")
@Slf4j
public class CmdSinks extends CmdBase {

    private final CreateSink createSink;
    private final UpdateSink updateSink;
    private final DeleteSink deleteSink;
    private final ListSinks listSinks;
    private final GetSink getSink;
    private final GetSinkStatus getSinkStatus;
    private final StopSink stopSink;
    private final StartSink startSink;
    private final RestartSink restartSink;
    private final LocalSinkRunner localSinkRunner;

    public CmdSinks(PulsarAdmin admin) {
<<<<<<< HEAD
        super("sink", admin);
=======
        super("sinks", admin);
>>>>>>> f773c602c... Test pr 10 (#27)
        createSink = new CreateSink();
        updateSink = new UpdateSink();
        deleteSink = new DeleteSink();
        listSinks = new ListSinks();
        getSink = new GetSink();
        getSinkStatus = new GetSinkStatus();
        stopSink = new StopSink();
        startSink = new StartSink();
        restartSink = new RestartSink();
        localSinkRunner = new LocalSinkRunner();

        jcommander.addCommand("create", createSink);
        jcommander.addCommand("update", updateSink);
        jcommander.addCommand("delete", deleteSink);
        jcommander.addCommand("list", listSinks);
        jcommander.addCommand("get", getSink);
        // TODO deprecate getstatus
        jcommander.addCommand("status", getSinkStatus, "getstatus");
        jcommander.addCommand("stop", stopSink);
        jcommander.addCommand("start", startSink);
        jcommander.addCommand("restart", restartSink);
        jcommander.addCommand("localrun", localSinkRunner);
        jcommander.addCommand("available-sinks", new ListBuiltInSinks());
<<<<<<< HEAD
=======
        jcommander.addCommand("reload", new ReloadBuiltInSinks());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

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

        void processArguments() throws Exception {
        }

        abstract void runCmd() throws Exception;
    }

    @Parameters(commandDescription = "Run a Pulsar IO sink connector locally (rather than deploying it to the Pulsar cluster)")
    protected class LocalSinkRunner extends CreateSink {

<<<<<<< HEAD
=======
        @Parameter(names = "--state-storage-service-url", description = "The URL for the state storage service (the default is Apache BookKeeper)")
        protected String stateStorageServiceUrl;
>>>>>>> f773c602c... Test pr 10 (#27)
        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker", hidden = true)
        protected String DEPRECATED_brokerServiceUrl;
        @Parameter(names = "--broker-service-url", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using which function-process can connect to broker", hidden = true)
        protected String DEPRECATED_clientAuthPlugin;
        @Parameter(names = "--client-auth-plugin", description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;

        @Parameter(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
        protected String DEPRECATED_clientAuthParams;
        @Parameter(names = "--client-auth-params", description = "Client authentication param")
        protected String clientAuthParams;

        @Parameter(names = "--use_tls", description = "Use tls connection", hidden = true)
        protected Boolean DEPRECATED_useTls;
        @Parameter(names = "--use-tls", description = "Use tls connection")
        protected boolean useTls;

        @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection", hidden = true)
        protected Boolean DEPRECATED_tlsAllowInsecureConnection;
        @Parameter(names = "--tls-allow-insecure", description = "Allow insecure tls connection")
        protected boolean tlsAllowInsecureConnection;

        @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification", hidden = true)
        protected Boolean DEPRECATED_tlsHostNameVerificationEnabled;
        @Parameter(names = "--hostname-verification-enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;

        @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path", hidden = true)
        protected String DEPRECATED_tlsTrustCertFilePath;
        @Parameter(names = "--tls-trust-cert-path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;

<<<<<<< HEAD
=======
        @Parameter(names = "--secrets-provider-classname", description = "Whats the classname for secrets provider")
        protected String secretsProviderClassName;
        @Parameter(names = "--secrets-provider-config", description = "Config that needs to be passed to secrets provider")
        protected String secretsProviderConfig;

>>>>>>> f773c602c... Test pr 10 (#27)
        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_brokerServiceUrl)) brokerServiceUrl = DEPRECATED_brokerServiceUrl;
            if (!StringUtils.isBlank(DEPRECATED_clientAuthPlugin)) clientAuthPlugin = DEPRECATED_clientAuthPlugin;
            if (!StringUtils.isBlank(DEPRECATED_clientAuthParams)) clientAuthParams = DEPRECATED_clientAuthParams;
            if (DEPRECATED_useTls != null) useTls = DEPRECATED_useTls;
            if (DEPRECATED_tlsAllowInsecureConnection != null) tlsAllowInsecureConnection = DEPRECATED_tlsAllowInsecureConnection;
            if (DEPRECATED_tlsHostNameVerificationEnabled != null) tlsHostNameVerificationEnabled = DEPRECATED_tlsHostNameVerificationEnabled;
            if (!StringUtils.isBlank(DEPRECATED_tlsTrustCertFilePath)) tlsTrustCertFilePath = DEPRECATED_tlsTrustCertFilePath;
        }

        @Override
        public void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            List<String> localRunArgs = new LinkedList<>();
            localRunArgs.add(System.getenv("PULSAR_HOME") + "/bin/function-localrunner");
            localRunArgs.add("--sinkConfig");
            localRunArgs.add(new Gson().toJson(sinkConfig));
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

        @Override
        protected String validateSinkType(String sinkType) {
            return sinkType;
        }
    }

    @Parameters(commandDescription = "Submit a Pulsar IO sink connector to run in a Pulsar cluster")
    protected class CreateSink extends SinkDetailsCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(archive)) {
<<<<<<< HEAD
                admin.sink().createSinkWithUrl(sinkConfig, sinkConfig.getArchive());
            } else {
                admin.sink().createSink(sinkConfig, sinkConfig.getArchive());
=======
                admin.sinks().createSinkWithUrl(sinkConfig, sinkConfig.getArchive());
            } else {
                admin.sinks().createSink(sinkConfig, sinkConfig.getArchive());
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar IO sink connector")
    protected class UpdateSink extends SinkDetailsCommand {
<<<<<<< HEAD
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                admin.sink().updateSinkWithUrl(sinkConfig, sinkConfig.getArchive());
            } else {
                admin.sink().updateSink(sinkConfig, sinkConfig.getArchive());
=======

        @Parameter(names = "--update-auth-data", description = "Whether or not to update the auth data")
        protected boolean updateAuthData;

        @Override
        void runCmd() throws Exception {
            UpdateOptions updateOptions = new UpdateOptions();
            updateOptions.setUpdateAuthData(updateAuthData);
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                admin.sinks().updateSinkWithUrl(sinkConfig, sinkConfig.getArchive(), updateOptions);
            } else {
                admin.sinks().updateSink(sinkConfig, sinkConfig.getArchive(), updateOptions);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            print("Updated successfully");
        }

        protected void validateSinkConfigs(SinkConfig sinkConfig) {
<<<<<<< HEAD
            org.apache.pulsar.common.functions.Utils.inferMissingArguments(sinkConfig);
=======
            if (sinkConfig.getTenant() == null) {
                sinkConfig.setTenant(PUBLIC_TENANT);
            }
            if (sinkConfig.getNamespace() == null) {
                sinkConfig.setNamespace(DEFAULT_NAMESPACE);
            }
>>>>>>> f773c602c... Test pr 10 (#27)
        }
    }

    abstract class SinkDetailsCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The sink's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The sink's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The sink's name")
        protected String name;

        @Parameter(names = { "-t", "--sink-type" }, description = "The sinks's connector provider")
        protected String sinkType;

        @Parameter(names = { "-i",
                "--inputs" }, description = "The sink's input topic or topics (multiple topics can be specified as a comma-separated list)")
        protected String inputs;

        @Parameter(names = "--topicsPattern", description = "TopicsPattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topicsPattern] are mutually exclusive. Add SerDe class name for a pattern in --customSerdeInputs  (supported for java fun only)", hidden = true)
        protected String DEPRECATED_topicsPattern;
        @Parameter(names = "--topics-pattern", description = "TopicsPattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topicsPattern] are mutually exclusive. Add SerDe class name for a pattern in --customSerdeInputs  (supported for java fun only)")
        protected String topicsPattern;

        @Parameter(names = "--subsName", description = "Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer", hidden = true)
        protected String DEPRECATED_subsName;
        @Parameter(names = "--subs-name", description = "Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer")
        protected String subsName;

<<<<<<< HEAD
=======
        @Parameter(names = "--subs-position", description = "Pulsar source subscription position if user wants to consume messages from the specified location")
        protected SubscriptionInitialPosition subsPosition;

>>>>>>> f773c602c... Test pr 10 (#27)
        @Parameter(names = "--customSerdeInputs", description = "The map of input topics to SerDe class names (as a JSON string)", hidden = true)
        protected String DEPRECATED_customSerdeInputString;
        @Parameter(names = "--custom-serde-inputs", description = "The map of input topics to SerDe class names (as a JSON string)")
        protected String customSerdeInputString;

        @Parameter(names = "--custom-schema-inputs", description = "The map of input topics to Schema types or class names (as a JSON string)")
        protected String customSchemaInputString;

<<<<<<< HEAD
=======
        @Parameter(names = "--input-specs", description = "The map of inputs to custom configuration (as a JSON string)")
        protected String inputSpecs;

        @Parameter(names = "--max-redeliver-count", description = "Maximum number of times that a message will be redelivered before being sent to the dead letter queue")
        protected Integer maxMessageRetries;
        @Parameter(names = "--dead-letter-topic", description = "Name of the dead topic where the failing messages will be sent.")
        protected String deadLetterTopic;
>>>>>>> f773c602c... Test pr 10 (#27)

        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the sink", hidden = true)
        protected FunctionConfig.ProcessingGuarantees DEPRECATED_processingGuarantees;
        @Parameter(names = "--processing-guarantees", description = "The processing guarantees (aka delivery semantics) applied to the sink")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--retainOrdering", description = "Sink consumes and sinks messages in order", hidden = true)
        protected Boolean DEPRECATED_retainOrdering;
        @Parameter(names = "--retain-ordering", description = "Sink consumes and sinks messages in order")
        protected Boolean retainOrdering;
        @Parameter(names = "--parallelism", description = "The sink's parallelism factor (i.e. the number of sink instances to run)")
        protected Integer parallelism;
        @Parameter(names = {"-a", "--archive"}, description = "Path to the archive file for the sink. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
        protected String archive;
        @Parameter(names = "--className", description = "The sink's class name if archive is file-url-path (file://)", hidden = true)
        protected String DEPRECATED_className;
        @Parameter(names = "--classname", description = "The sink's class name if archive is file-url-path (file://)")
        protected String className;

        @Parameter(names = "--sinkConfigFile", description = "The path to a YAML config file specifying the "
                + "sink's configuration", hidden = true)
        protected String DEPRECATED_sinkConfigFile;
        @Parameter(names = "--sink-config-file", description = "The path to a YAML config file specifying the "
                + "sink's configuration")
        protected String sinkConfigFile;
        @Parameter(names = "--cpu", description = "The CPU (in cores) that needs to be allocated per sink instance (applicable only to Docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The RAM (in bytes) that need to be allocated per sink instance (applicable only to the process and Docker runtimes)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk (in bytes) that need to be allocated per sink instance (applicable only to Docker runtime)")
        protected Long disk;
        @Parameter(names = "--sinkConfig", description = "User defined configs key/values", hidden = true)
        protected String DEPRECATED_sinkConfigString;
        @Parameter(names = "--sink-config", description = "User defined configs key/values")
        protected String sinkConfigString;
<<<<<<< HEAD
        @Parameter(names = "--auto-ack", description = "Whether or not the framework will automatically acknowleges messages", arity = 1)
        protected Boolean autoAck;
        @Parameter(names = "--timeout-ms", description = "The message timeout in milliseconds")
        protected Long timeoutMs;
=======
        @Parameter(names = "--auto-ack", description = "Whether or not the framework will automatically acknowledge messages", arity = 1)
        protected Boolean autoAck;
        @Parameter(names = "--timeout-ms", description = "The message timeout in milliseconds")
        protected Long timeoutMs;
        @Parameter(names = "--negative-ack-redelivery-delay-ms", description = "The negative ack message redelivery delay in milliseconds")
        protected Long negativeAckRedeliveryDelayMs;
        @Parameter(names = "--custom-runtime-options", description = "A string that encodes options to customize the runtime, see docs for configured runtime for details")
        protected String customRuntimeOptions;
>>>>>>> f773c602c... Test pr 10 (#27)

        protected SinkConfig sinkConfig;

        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_subsName)) subsName = DEPRECATED_subsName;
            if (!StringUtils.isBlank(DEPRECATED_topicsPattern)) topicsPattern = DEPRECATED_topicsPattern;
            if (!StringUtils.isBlank(DEPRECATED_customSerdeInputString)) customSerdeInputString = DEPRECATED_customSerdeInputString;
            if (DEPRECATED_processingGuarantees != null) processingGuarantees = DEPRECATED_processingGuarantees;
            if (DEPRECATED_retainOrdering != null) retainOrdering = DEPRECATED_retainOrdering;
            if (!StringUtils.isBlank(DEPRECATED_className)) className = DEPRECATED_className;
            if (!StringUtils.isBlank(DEPRECATED_sinkConfigFile)) sinkConfigFile = DEPRECATED_sinkConfigFile;
            if (!StringUtils.isBlank(DEPRECATED_sinkConfigString)) sinkConfigString = DEPRECATED_sinkConfigString;
        }

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            // merge deprecated args with new args
            mergeArgs();

            if (null != sinkConfigFile) {
                this.sinkConfig = CmdUtils.loadConfig(sinkConfigFile, SinkConfig.class);
<<<<<<< HEAD
                log.info("The sinkConfig read from file is {}", sinkConfig);
=======
>>>>>>> f773c602c... Test pr 10 (#27)
            } else {
                this.sinkConfig = new SinkConfig();
            }

            if (null != tenant) {
                sinkConfig.setTenant(tenant);
            }

            if (null != namespace) {
                sinkConfig.setNamespace(namespace);
            }

            if (null != className) {
                sinkConfig.setClassName(className);
            }

            if (null != name) {
                sinkConfig.setName(name);
            }
            if (null != processingGuarantees) {
                sinkConfig.setProcessingGuarantees(processingGuarantees);
            }

            if (retainOrdering != null) {
                sinkConfig.setRetainOrdering(retainOrdering);
            }

            if (null != inputs) {
                sinkConfig.setInputs(Arrays.asList(inputs.split(",")));
            }
            if (null != customSerdeInputString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> customSerdeInputMap = new Gson().fromJson(customSerdeInputString, type);
                sinkConfig.setTopicToSerdeClassName(customSerdeInputMap);
            }

            if (null != customSchemaInputString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> customSchemaInputMap = new Gson().fromJson(customSchemaInputString, type);
                sinkConfig.setTopicToSchemaType(customSchemaInputMap);
            }

<<<<<<< HEAD
=======
            if(null != inputSpecs){
                Type type = new TypeToken<Map<String, ConsumerConfig>>(){}.getType();
                sinkConfig.setInputSpecs(new Gson().fromJson(inputSpecs, type));
            }

            sinkConfig.setMaxMessageRetries(maxMessageRetries);
            if (null != deadLetterTopic) {
                sinkConfig.setDeadLetterTopic(deadLetterTopic);
            }

>>>>>>> f773c602c... Test pr 10 (#27)
            if (isNotBlank(subsName)) {
                sinkConfig.setSourceSubscriptionName(subsName);
            }

<<<<<<< HEAD
=======
            if (null != subsPosition) {
                sinkConfig.setSourceSubscriptionPosition(subsPosition);
            }

>>>>>>> f773c602c... Test pr 10 (#27)
            if (null != topicsPattern) {
                sinkConfig.setTopicsPattern(topicsPattern);
            }

            if (parallelism != null) {
                sinkConfig.setParallelism(parallelism);
            }

            if (archive != null && sinkType != null) {
                throw new ParameterException("Cannot specify both archive and sink-type");
            }

            if (null != archive) {
                sinkConfig.setArchive(archive);
            }

            if (sinkType != null) {
                sinkConfig.setArchive(validateSinkType(sinkType));
            }

            Resources resources = sinkConfig.getResources();
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
                sinkConfig.setResources(resources);
            }

            if (null != sinkConfigString) {
                sinkConfig.setConfigs(parseConfigs(sinkConfigString));
            }

            if (autoAck != null) {
                sinkConfig.setAutoAck(autoAck);
            }
            if (timeoutMs != null) {
                sinkConfig.setTimeoutMs(timeoutMs);
            }
<<<<<<< HEAD
            
            if (null != sinkConfigString) {
                sinkConfig.setConfigs(parseConfigs(sinkConfigString));
=======
            if (negativeAckRedeliveryDelayMs != null && negativeAckRedeliveryDelayMs > 0) {
                sinkConfig.setNegativeAckRedeliveryDelayMs(negativeAckRedeliveryDelayMs);
            }

            if (customRuntimeOptions != null) {
                sinkConfig.setCustomRuntimeOptions(customRuntimeOptions);
>>>>>>> f773c602c... Test pr 10 (#27)
            }

            // check if configs are valid
            validateSinkConfigs(sinkConfig);
        }

        protected Map<String, Object> parseConfigs(String str) {
<<<<<<< HEAD
            Type type = new TypeToken<Map<String, String>>(){}.getType();
=======
            Type type = new TypeToken<Map<String, Object>>(){}.getType();
>>>>>>> f773c602c... Test pr 10 (#27)
            return new Gson().fromJson(str, type);
        }

        protected void validateSinkConfigs(SinkConfig sinkConfig) {

            if (isBlank(sinkConfig.getArchive())) {
                throw new ParameterException("Sink archive not specfied");
            }

            org.apache.pulsar.common.functions.Utils.inferMissingArguments(sinkConfig);

            if (!Utils.isFunctionPackageUrlSupported(sinkConfig.getArchive()) &&
                    !sinkConfig.getArchive().startsWith(Utils.BUILTIN)) {
                if (!new File(sinkConfig.getArchive()).exists()) {
                    throw new IllegalArgumentException(String.format("Sink Archive file %s does not exist", sinkConfig.getArchive()));
                }
            }
        }

        protected String validateSinkType(String sinkType) throws IOException {
            Set<String> availableSinks;
            try {
<<<<<<< HEAD
                availableSinks = admin.sink().getBuiltInSinks().stream().map(ConnectorDefinition::getName).collect(Collectors.toSet());
=======
                availableSinks = admin.sinks().getBuiltInSinks().stream().map(ConnectorDefinition::getName).collect(Collectors.toSet());
>>>>>>> f773c602c... Test pr 10 (#27)
            } catch (PulsarAdminException e) {
                throw new IOException(e);
            }

            if (!availableSinks.contains(sinkType)) {
                throw new ParameterException(
                        "Invalid sink type '" + sinkType + "' -- Available sinks are: " + availableSinks);
            }

            // Source type is a valid built-in connector type
            return "builtin://" + sinkType;
        }
    }

    /**
     * Sink level command
     */
    @Getter
    abstract class SinkCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The sink's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The sink's namespace")
        protected String namespace;

        @Parameter(names = "--name", description = "The sink's name")
        protected String sinkName;

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
            if (null == sinkName) {
                throw new RuntimeException(
                        "You must specify a name for the sink");
            }
        }
    }

    @Parameters(commandDescription = "Stops a Pulsar IO sink connector")
    protected class DeleteSink extends SinkCommand {

        @Override
        void runCmd() throws Exception {
<<<<<<< HEAD
            admin.sink().deleteSink(tenant, namespace, sinkName);
=======
            admin.sinks().deleteSink(tenant, namespace, sinkName);
>>>>>>> f773c602c... Test pr 10 (#27)
            print("Deleted successfully");
        }
    }

    @Parameters(commandDescription = "Gets the information about a Pulsar IO sink connector")
    protected class GetSink extends SinkCommand {

        @Override
        void runCmd() throws Exception {
<<<<<<< HEAD
            SinkConfig sinkConfig = admin.sink().getSink(tenant, namespace, sinkName);
=======
            SinkConfig sinkConfig = admin.sinks().getSink(tenant, namespace, sinkName);
>>>>>>> f773c602c... Test pr 10 (#27)
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sinkConfig));
        }
    }

    /**
     * List Sources command
     */
    @Parameters(commandDescription = "List all running Pulsar IO sink connectors")
    protected class ListSinks extends BaseCommand {
        @Parameter(names = "--tenant", description = "The sink's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The sink's namespace")
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

        @Override
        void runCmd() throws Exception {
<<<<<<< HEAD
            List<String> sinks = admin.sink().listSinks(tenant, namespace);
=======
            List<String> sinks = admin.sinks().listSinks(tenant, namespace);
>>>>>>> f773c602c... Test pr 10 (#27)
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sinks));
        }
    }

    @Parameters(commandDescription = "Check the current status of a Pulsar Sink")
    class GetSinkStatus extends SinkCommand {

        @Parameter(names = "--instance-id", description = "The sink instanceId (Get-status of all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isBlank(instanceId)) {
<<<<<<< HEAD
                print(admin.sink().getSinkStatus(tenant, namespace, sinkName));
            } else {
                print(admin.sink().getSinkStatus(tenant, namespace, sinkName, Integer.parseInt(instanceId)));
=======
                print(admin.sinks().getSinkStatus(tenant, namespace, sinkName));
            } else {
                print(admin.sinks().getSinkStatus(tenant, namespace, sinkName, Integer.parseInt(instanceId)));
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        }
    }

    @Parameters(commandDescription = "Restart sink instance")
    class RestartSink extends SinkCommand {

        @Parameter(names = "--instance-id", description = "The sink instanceId (restart all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
<<<<<<< HEAD
                    admin.sink().restartSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
=======
                    admin.sinks().restartSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
>>>>>>> f773c602c... Test pr 10 (#27)
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
<<<<<<< HEAD
                admin.sink().restartSink(tenant, namespace, sinkName);
=======
                admin.sinks().restartSink(tenant, namespace, sinkName);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            System.out.println("Restarted successfully");
        }
    }

    @Parameters(commandDescription = "Stops sink instance")
    class StopSink extends SinkCommand {

        @Parameter(names = "--instance-id", description = "The sink instanceId (stop all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
<<<<<<< HEAD
                    admin.sink().stopSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
=======
                    admin.sinks().stopSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
>>>>>>> f773c602c... Test pr 10 (#27)
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
<<<<<<< HEAD
                admin.sink().stopSink(tenant, namespace, sinkName);
=======
                admin.sinks().stopSink(tenant, namespace, sinkName);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            System.out.println("Stopped successfully");
        }
    }

    @Parameters(commandDescription = "Starts sink instance")
    class StartSink extends SinkCommand {

        @Parameter(names = "--instance-id", description = "The sink instanceId (start all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
<<<<<<< HEAD
                    admin.sink().startSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
=======
                    admin.sinks().startSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
>>>>>>> f773c602c... Test pr 10 (#27)
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
<<<<<<< HEAD
                admin.sink().startSink(tenant, namespace, sinkName);
=======
                admin.sinks().startSink(tenant, namespace, sinkName);
>>>>>>> f773c602c... Test pr 10 (#27)
            }
            System.out.println("Started successfully");
        }
    }

    @Parameters(commandDescription = "Get the list of Pulsar IO connector sinks supported by Pulsar cluster")
    public class ListBuiltInSinks extends BaseCommand {
        @Override
        void runCmd() throws Exception {
<<<<<<< HEAD
            admin.sink().getBuiltInSinks().stream().filter(x -> isNotBlank(x.getSinkClass()))
=======
            admin.sinks().getBuiltInSinks().stream().filter(x -> isNotBlank(x.getSinkClass()))
>>>>>>> f773c602c... Test pr 10 (#27)
                    .forEach(connector -> {
                        System.out.println(connector.getName());
                        System.out.println(WordUtils.wrap(connector.getDescription(), 80));
                        System.out.println("----------------------------------------");
                    });
        }
    }
<<<<<<< HEAD
=======

    @Parameters(commandDescription = "Reload the available built-in connectors")
    public class ReloadBuiltInSinks extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            admin.sinks().reloadBuiltInSinks();
        }
    }
>>>>>>> f773c602c... Test pr 10 (#27)
}
