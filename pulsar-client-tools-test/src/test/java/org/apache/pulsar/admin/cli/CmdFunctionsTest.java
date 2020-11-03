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


<<<<<<< HEAD
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
=======
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
>>>>>>> f773c602c... Test pr 10 (#27)
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
<<<<<<< HEAD
import java.util.LinkedList;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.pulsar.admin.cli.CmdFunctions.CreateFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.DeleteFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.GetFunctionStatus;
import org.apache.pulsar.admin.cli.CmdFunctions.ListFunctions;
import org.apache.pulsar.admin.cli.CmdFunctions.RestartFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.StateGetter;
import org.apache.pulsar.admin.cli.CmdFunctions.StopFunction;
import org.apache.pulsar.admin.cli.CmdFunctions.UpdateFunction;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.functions.FunctionConfig;
<<<<<<< HEAD
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Utils;
=======
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.utils.FunctionCommon;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link CmdFunctions}.
 */
@Slf4j
<<<<<<< HEAD
@PrepareForTest({ CmdFunctions.class, Reflections.class, StorageClientBuilder.class, Utils.class})
=======
@PrepareForTest({ CmdFunctions.class, Reflections.class, StorageClientBuilder.class, FunctionCommon.class})
>>>>>>> f773c602c... Test pr 10 (#27)
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
public class CmdFunctionsTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String TEST_NAME = "test_name";
    private static final String JAR_NAME = CmdFunctionsTest.class.getClassLoader().getResource("dummyexamples.jar").getFile();
<<<<<<< HEAD
=======
    private static final String URL ="file:" + JAR_NAME;
    private static final String FN_NAME = TEST_NAME + "-function";
    private static final String INPUT_TOPIC_NAME = TEST_NAME + "-input-topic";
    private static final String OUTPUT_TOPIC_NAME = TEST_NAME + "-output-topic";
    private static final String TENANT = TEST_NAME + "-tenant";
    private static final String NAMESPACE = TEST_NAME + "-namespace";
>>>>>>> f773c602c... Test pr 10 (#27)

    private PulsarAdmin admin;
    private Functions functions;
    private CmdFunctions cmd;
    private CmdSinks cmdSinks;
    private CmdSources cmdSources;

    public static class DummyFunction implements Function<String, String> {

        public DummyFunction() {

        }

        @Override
        public String process(String input, Context context) throws Exception {
            return null;
        }
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.admin = mock(PulsarAdmin.class);
        this.functions = mock(Functions.class);
        when(admin.functions()).thenReturn(functions);
        when(admin.getServiceUrl()).thenReturn("http://localhost:1234");
        when(admin.getClientConfigData()).thenReturn(new ClientConfigurationData());
        this.cmd = new CmdFunctions(admin);
        this.cmdSinks = new CmdSinks(admin);
        this.cmdSources = new CmdSources(admin);

        // mock reflections
        mockStatic(Reflections.class);
        when(Reflections.classExistsInJar(any(File.class), anyString())).thenReturn(true);
        when(Reflections.classExists(anyString())).thenReturn(true);
        when(Reflections.classInJarImplementsIface(any(File.class), anyString(), eq(Function.class)))
            .thenReturn(true);
        when(Reflections.classImplementsIface(anyString(), any())).thenReturn(true);
        when(Reflections.createInstance(eq(DummyFunction.class.getName()), any(File.class))).thenReturn(new DummyFunction());
    }

//    @Test
//    public void testLocalRunnerCmdNoArguments() throws Exception {
//        cmd.run(new String[] { "run" });
//
//        LocalRunner runner = cmd.getLocalRunner();
//        assertNull(runner.getFunctionName());
//        assertNull(runner.getInputs());
//        assertNull(runner.getOutput());
//        assertNull(runner.getFnConfigFile());
//    }

    /*
    TODO(sijie):- Can we fix this?
    @Test
    public void testLocalRunnerCmdSettings() throws Exception {
        String fnName = TEST_NAME + "-function";
        String sourceTopicName = TEST_NAME + "-source-topic";
        String output = TEST_NAME + "-sink-topic";
        cmd.run(new String[] {
            "localrun",
            "--name", fnName,
            "--source-topics", sourceTopicName,
            "--output", output
        });

        LocalRunner runner = cmd.getLocalRunner();
        assertEquals(fnName, runner.getFunctionName());
        assertEquals(sourceTopicName, runner.getInputs());
        assertEquals(output, runner.getOutput());
        assertNull(runner.getFnConfigFile());
    }

    @Test
    public void testLocalRunnerCmdYaml() throws Exception {
        URL yamlUrl = getClass().getClassLoader().getResource("test_function_config.yml");
        String configFile = yamlUrl.getPath();
        cmd.run(new String[] {
            "localrun",
            "--function-config", configFile
        });

        LocalRunner runner = cmd.getLocalRunner();
        assertNull(runner.getFunctionName());
        assertNull(runner.getInputs());
        assertNull(runner.getOutput());
        assertEquals(configFile, runner.getFnConfigFile());
    }
    */

    @Test
    public void testCreateFunction() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";
        cmd.run(new String[] {
            "create",
            "--name", fnName,
            "--inputs", inputTopicName,
            "--output", outputTopicName,
=======
        cmd.run(new String[] {
            "create",
            "--name", FN_NAME,
            "--inputs", INPUT_TOPIC_NAME,
            "--output", OUTPUT_TOPIC_NAME,
>>>>>>> f773c602c... Test pr 10 (#27)
            "--jar", JAR_NAME,
            "--auto-ack", "false",
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
<<<<<<< HEAD
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(new Boolean(false), creater.getAutoAck());
=======
            "--dead-letter-topic", "test-dead-letter-topic",
            "--custom-runtime-options", "custom-runtime-options"
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(Boolean.FALSE, creater.getAutoAck());
        assertEquals("test-dead-letter-topic", creater.getDeadLetterTopic());
        assertEquals("custom-runtime-options", creater.getCustomRuntimeOptions());
>>>>>>> f773c602c... Test pr 10 (#27)

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void restartFunction() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(fnName, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, fnName, instanceId);
=======
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(FN_NAME, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, FN_NAME, instanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void restartFunctionInstances() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", fnName });

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(fnName, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, fnName);
=======
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "restart", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME});

        RestartFunction restarter = cmd.getRestarter();
        assertEquals(FN_NAME, restarter.getFunctionName());

        verify(functions, times(1)).restartFunction(tenant, namespace, FN_NAME);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void stopFunction() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        StopFunction stop = cmd.getStopper();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, fnName, instanceId);
=======
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        StopFunction stop = cmd.getStopper();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, FN_NAME, instanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void stopFunctionInstances() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", fnName });

        StopFunction stop = cmd.getStopper();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, fnName);
=======
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "stop", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME});

        StopFunction stop = cmd.getStopper();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).stopFunction(tenant, namespace, FN_NAME);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void startFunction() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, fnName, instanceId);
=======
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, FN_NAME, instanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void startFunctionInstances() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", fnName });

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(fnName, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, fnName);
=======
        String tenant = "sample";
        String namespace = "ns1";
        cmd.run(new String[] { "start", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME});

        CmdFunctions.StartFunction stop = cmd.getStarter();
        assertEquals(FN_NAME, stop.getFunctionName());

        verify(functions, times(1)).startFunction(tenant, namespace, FN_NAME);
>>>>>>> f773c602c... Test pr 10 (#27)
    }


    @Test
    public void testGetFunctionStatus() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "getstatus", "--tenant", tenant, "--namespace", namespace, "--name", fnName,
                "--instance-id", Integer.toString(instanceId)});

        GetFunctionStatus status = cmd.getStatuser();
        assertEquals(fnName, status.getFunctionName());

        verify(functions, times(1)).getFunctionStatus(tenant, namespace, fnName, instanceId);
=======
        String tenant = "sample";
        String namespace = "ns1";
        int instanceId = 0;
        cmd.run(new String[] { "getstatus", "--tenant", tenant, "--namespace", namespace, "--name", FN_NAME,
                "--instance-id", Integer.toString(instanceId)});

        GetFunctionStatus status = cmd.getStatuser();
        assertEquals(FN_NAME, status.getFunctionName());

        verify(functions, times(1)).getFunctionStatus(tenant, namespace, FN_NAME, instanceId);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testCreateFunctionWithFileUrl() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
            "create",
            "--name", fnName,
            "--inputs", inputTopicName,
            "--output", outputTopicName,
            "--jar", url,
=======
        cmd.run(new String[] {
            "create",
            "--name", FN_NAME,
            "--inputs", INPUT_TOPIC_NAME,
            "--output", OUTPUT_TOPIC_NAME,
            "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

<<<<<<< HEAD
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
=======
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
>>>>>>> f773c602c... Test pr 10 (#27)
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithoutBasicArguments() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
=======
        cmd.run(new String[] {
                "create",
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--className", IdentityFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();

        assertEquals("IdentityFunction", creater.getFunctionConfig().getName());
        assertEquals("public", creater.getFunctionConfig().getTenant());
        assertEquals("default", creater.getFunctionConfig().getNamespace());

<<<<<<< HEAD
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
=======
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
>>>>>>> f773c602c... Test pr 10 (#27)
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithTopicPatterns() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String topicPatterns = "persistent://tenant/ns/topicPattern*";
        String outputTopicName = TEST_NAME + "-output-topic";
        cmd.run(new String[] {
            "create",
            "--name", fnName,
            "--topicsPattern", topicPatterns,
            "--output", outputTopicName,
=======
        String topicPatterns = "persistent://tenant/ns/topicPattern*";
        cmd.run(new String[] {
            "create",
            "--name", FN_NAME,
            "--topicsPattern", topicPatterns,
            "--output", OUTPUT_TOPIC_NAME,
>>>>>>> f773c602c... Test pr 10 (#27)
            "--jar", JAR_NAME,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
<<<<<<< HEAD
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(topicPatterns, creater.getTopicsPattern());
        assertEquals(outputTopicName, creater.getOutput());
=======
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(topicPatterns, creater.getTopicsPattern());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
>>>>>>> f773c602c... Test pr 10 (#27)

        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }

    @Test
    public void testCreateUsingFullyQualifiedFunctionName() throws Exception {
<<<<<<< HEAD
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";
=======
>>>>>>> f773c602c... Test pr 10 (#27)
        String tenant = "sample";
        String namespace = "ns1";
        String functionName = "func";
        String fqfn = String.format("%s/%s/%s", tenant, namespace, functionName);

        cmd.run(new String[] {
                "create",
<<<<<<< HEAD
                "--inputs", inputTopicName,
                "--output", outputTopicName,
=======
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--fqfn", fqfn,
                "--jar", JAR_NAME,
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertEquals(tenant, creater.getFunctionConfig().getTenant());
        assertEquals(namespace, creater.getFunctionConfig().getNamespace());
        assertEquals(functionName, creater.getFunctionConfig().getName());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateWithoutOutputTopicWithSkipFlag() throws Exception {
<<<<<<< HEAD
        String inputTopicName = TEST_NAME + "-input-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
=======
        cmd.run(new String[] {
                "create",
                "--inputs", INPUT_TOPIC_NAME,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--jar", JAR_NAME,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        assertNull(creater.getFunctionConfig().getOutput());
        verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());

    }


    @Test
    public void testCreateWithoutOutputTopic() {

        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();

<<<<<<< HEAD
        String inputTopicName = TEST_NAME + "-input-topic";
        cmd.run(new String[] {
                "create",
                "--inputs", inputTopicName,
=======
        cmd.run(new String[] {
                "create",
                "--inputs", INPUT_TOPIC_NAME,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--jar", JAR_NAME,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
        });

        CreateFunction creater = cmd.getCreater();
        consoleOutputCapturer.stop();
        assertNull(creater.getFunctionConfig().getOutput());
        assertTrue(consoleOutputCapturer.getStdout().contains("Created successfully"));
    }

    @Test
    public void testGetFunction() throws Exception {
<<<<<<< HEAD
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";
        String fnName = TEST_NAME + "-function";

        cmd.run(new String[] {
            "get",
            "--name", fnName,
            "--tenant", tenant,
            "--namespace", namespace
        });

        GetFunction getter = cmd.getGetter();
        assertEquals(fnName, getter.getFunctionName());
        assertEquals(tenant, getter.getTenant());
        assertEquals(namespace, getter.getNamespace());

        verify(functions, times(1)).getFunction(eq(tenant), eq(namespace), eq(fnName));
=======
        cmd.run(new String[] {
            "get",
            "--name", FN_NAME,
            "--tenant", TENANT,
            "--namespace", NAMESPACE
        });

        GetFunction getter = cmd.getGetter();
        assertEquals(FN_NAME, getter.getFunctionName());
        assertEquals(TENANT, getter.getTenant());
        assertEquals(NAMESPACE, getter.getNamespace());

        verify(functions, times(1)).getFunction(eq(TENANT), eq(NAMESPACE), eq(FN_NAME));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testDeleteFunction() throws Exception {
<<<<<<< HEAD
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";
        String fnName = TEST_NAME + "-function";

        cmd.run(new String[] {
            "delete",
            "--name", fnName,
            "--tenant", tenant,
            "--namespace", namespace
        });

        DeleteFunction deleter = cmd.getDeleter();
        assertEquals(fnName, deleter.getFunctionName());
        assertEquals(tenant, deleter.getTenant());
        assertEquals(namespace, deleter.getNamespace());

        verify(functions, times(1)).deleteFunction(eq(tenant), eq(namespace), eq(fnName));
=======
        cmd.run(new String[] {
            "delete",
            "--name", FN_NAME,
            "--tenant", TENANT,
            "--namespace", NAMESPACE
        });

        DeleteFunction deleter = cmd.getDeleter();
        assertEquals(FN_NAME, deleter.getFunctionName());
        assertEquals(TENANT, deleter.getTenant());
        assertEquals(NAMESPACE, deleter.getNamespace());

        verify(functions, times(1)).deleteFunction(eq(TENANT), eq(NAMESPACE), eq(FN_NAME));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testUpdateFunction() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        cmd.run(new String[] {
            "update",
            "--name", fnName,
            "--inputs", inputTopicName,
            "--output", outputTopicName,
=======
        cmd.run(new String[] {
            "update",
            "--name", FN_NAME,
            "--inputs", INPUT_TOPIC_NAME,
            "--output", OUTPUT_TOPIC_NAME,
>>>>>>> f773c602c... Test pr 10 (#27)
            "--jar", JAR_NAME,
            "--tenant", "sample",
            "--namespace", "ns1",
            "--className", DummyFunction.class.getName(),
        });

        UpdateFunction updater = cmd.getUpdater();
<<<<<<< HEAD
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());

        verify(functions, times(1)).updateFunction(any(FunctionConfig.class), anyString());
=======
        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());

        verify(functions, times(1)).updateFunction(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testListFunctions() throws Exception {
<<<<<<< HEAD
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";

        cmd.run(new String[] {
            "list",
            "--tenant", tenant,
            "--namespace", namespace
        });

        ListFunctions lister = cmd.getLister();
        assertEquals(tenant, lister.getTenant());
        assertEquals(namespace, lister.getNamespace());

        verify(functions, times(1)).getFunctions(eq(tenant), eq(namespace));
=======
        cmd.run(new String[] {
            "list",
            "--tenant", TENANT,
            "--namespace", NAMESPACE
        });

        ListFunctions lister = cmd.getLister();
        assertEquals(TENANT, lister.getTenant());
        assertEquals(NAMESPACE, lister.getNamespace());

        verify(functions, times(1)).getFunctions(eq(TENANT), eq(NAMESPACE));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testStateGetter() throws Exception {
<<<<<<< HEAD
        String tenant = TEST_NAME + "-tenant";
        String namespace = TEST_NAME + "-namespace";
        String fnName = TEST_NAME + "-function";
=======
>>>>>>> f773c602c... Test pr 10 (#27)
        String key = TEST_NAME + "-key";

        cmd.run(new String[] {
            "querystate",
<<<<<<< HEAD
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", fnName,
=======
            "--tenant", TENANT,
            "--namespace", NAMESPACE,
            "--name", FN_NAME,
>>>>>>> f773c602c... Test pr 10 (#27)
            "--key", key
        });

        StateGetter stateGetter = cmd.getStateGetter();

<<<<<<< HEAD
        assertEquals(tenant, stateGetter.getTenant());
        assertEquals(namespace, stateGetter.getNamespace());
        assertEquals(fnName, stateGetter.getFunctionName());

        verify(functions, times(1)).getFunctionState(eq(tenant), eq(namespace), eq(fnName), eq(key));
    }

    private static final String fnName = TEST_NAME + "-function";
    private static final String inputTopicName = TEST_NAME + "-input-topic";
    private static final String outputTopicName = TEST_NAME + "-output-topic";

    private void testValidateFunctionsConfigs(String[] correctArgs, String[] incorrectArgs,
                                              String errMessageCheck) throws Exception {

        String[] cmds = {"create", "update", "localrun"};

        for (String type : cmds) {
            List<String> correctArgList = new LinkedList<>();
            List<String> incorrectArgList = new LinkedList<>();
            correctArgList.add(type);
            incorrectArgList.add(type);

            correctArgList.addAll(Arrays.asList(correctArgs));
            incorrectArgList.addAll(Arrays.asList(incorrectArgs));
            cmd.run(correctArgList.toArray(new String[correctArgList.size()]));

            if (type.equals("create")) {
                CreateFunction creater = cmd.getCreater();
                assertEquals(fnName, creater.getFunctionName());
                assertEquals(inputTopicName, creater.getInputs());
                assertEquals(outputTopicName, creater.getOutput());
            } else if (type.equals("update")){
                UpdateFunction updater = cmd.getUpdater();
                assertEquals(fnName, updater.getFunctionName());
                assertEquals(inputTopicName, updater.getInputs());
                assertEquals(outputTopicName, updater.getOutput());
            } else {
                CmdFunctions.LocalRunner localRunner = cmd.getLocalRunner();
                assertEquals(fnName, localRunner.getFunctionName());
                assertEquals(inputTopicName, localRunner.getInputs());
                assertEquals(outputTopicName, localRunner.getOutput());
            }

            if (type.equals("create")) {
                verify(functions, times(1)).createFunction(any(FunctionConfig.class), anyString());
            } else if (type.equals("update")) {
                verify(functions, times(1)).updateFunction(any(FunctionConfig.class), anyString());
            }

            setup();
            ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
            consoleOutputCapturer.start();
            cmd.run(incorrectArgList.toArray(new String[incorrectArgList.size()]));

            consoleOutputCapturer.stop();
            String output = consoleOutputCapturer.getStderr();
            assertTrue(output.replace("\n", "").contains(errMessageCheck));
        }
=======
        assertEquals(TENANT, stateGetter.getTenant());
        assertEquals(NAMESPACE, stateGetter.getNamespace());
        assertEquals(FN_NAME, stateGetter.getFunctionName());

        verify(functions, times(1)).getFunctionState(eq(TENANT), eq(NAMESPACE), eq(FN_NAME), eq(key));
    }

    @Test
    public void testStateGetterWithoutKey() throws Exception {
        ConsoleOutputCapturer consoleOutputCapturer = new ConsoleOutputCapturer();
        consoleOutputCapturer.start();
        cmd.run(new String[] {
                "querystate",
                "--tenant", TENANT,
                "--namespace", NAMESPACE,
                "--name", FN_NAME,
        });
        consoleOutputCapturer.stop();
        String output = consoleOutputCapturer.getStderr();
        assertTrue(output.replace("\n", "").contains("State key needs to be specified"));
        StateGetter stateGetter = cmd.getStateGetter();
        assertEquals(TENANT, stateGetter.getTenant());
        assertEquals(NAMESPACE, stateGetter.getNamespace());
        assertEquals(FN_NAME, stateGetter.getFunctionName());
        verify(functions, times(0)).getFunctionState(any(), any(), any(), any());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testCreateFunctionWithCpu() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
=======
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--cpu", "5.0"
        });

        CreateFunction creater = cmd.getCreater();

<<<<<<< HEAD
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 5.0);
        // Disk/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
=======
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 5.0);
        // Disk/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
>>>>>>> f773c602c... Test pr 10 (#27)
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithRam() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
=======
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--ram", "5656565656"
        });

        CreateFunction creater = cmd.getCreater();

<<<<<<< HEAD
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getRam(), new Long(5656565656l));
        // cpu/disk should be default
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 1.0);
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
=======
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getRam(), Long.valueOf(5656565656L));
        // cpu/disk should be default
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 1.0);
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
>>>>>>> f773c602c... Test pr 10 (#27)
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }

    @Test
    public void testCreateFunctionWithDisk() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "create",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
=======
        cmd.run(new String[] {
                "create",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080"
        });

        CreateFunction creater = cmd.getCreater();

<<<<<<< HEAD
        assertEquals(fnName, creater.getFunctionName());
        assertEquals(inputTopicName, creater.getInputs());
        assertEquals(outputTopicName, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), new Long(8080808080808080l));
        // cpu/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
=======
        assertEquals(FN_NAME, creater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, creater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, creater.getOutput());
        assertEquals(creater.getFunctionConfig().getResources().getDisk(), Long.valueOf(8080808080808080L));
        // cpu/Ram should be default
        assertEquals(creater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
>>>>>>> f773c602c... Test pr 10 (#27)
        assertEquals(creater.getFunctionConfig().getResources().getCpu(), 1.0);
        verify(functions, times(1)).createFunctionWithUrl(any(FunctionConfig.class), anyString());
    }


    @Test
    public void testUpdateFunctionWithCpu() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "update",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
=======
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--cpu", "5.0"
        });

        UpdateFunction updater = cmd.getUpdater();

<<<<<<< HEAD
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 5.0);
        // Disk/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString());
=======
        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 5.0);
        // Disk/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testUpdateFunctionWithRam() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "update",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
=======
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--ram", "5656565656"
        });

        UpdateFunction updater = cmd.getUpdater();

<<<<<<< HEAD
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getRam(), new Long(5656565656l));
        // cpu/disk should be default
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), new Long(10737418240l));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString());
=======
        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(5656565656L));
        // cpu/disk should be default
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(10737418240L));
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Test
    public void testUpdateFunctionWithDisk() throws Exception {
<<<<<<< HEAD
        String fnName = TEST_NAME + "-function";
        String inputTopicName = TEST_NAME + "-input-topic";
        String outputTopicName = TEST_NAME + "-output-topic";

        final String url = "file:" + JAR_NAME;
        cmd.run(new String[] {
                "update",
                "--name", fnName,
                "--inputs", inputTopicName,
                "--output", outputTopicName,
                "--jar", url,
=======
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
>>>>>>> f773c602c... Test pr 10 (#27)
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080"
        });

        UpdateFunction updater = cmd.getUpdater();

<<<<<<< HEAD
        assertEquals(fnName, updater.getFunctionName());
        assertEquals(inputTopicName, updater.getInputs());
        assertEquals(outputTopicName, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), new Long(8080808080808080l));
        // cpu/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), new Long(1073741824l));
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString());
=======
        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(8080808080808080L));
        // cpu/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(new UpdateOptions()));
    }

    @Test
    public void testUpdateAuthData() throws Exception {
        cmd.run(new String[] {
                "update",
                "--name", FN_NAME,
                "--inputs", INPUT_TOPIC_NAME,
                "--output", OUTPUT_TOPIC_NAME,
                "--jar", URL,
                "--tenant", "sample",
                "--namespace", "ns1",
                "--className", DummyFunction.class.getName(),
                "--disk", "8080808080808080",
                "--update-auth-data"
        });

        UpdateFunction updater = cmd.getUpdater();

        assertEquals(FN_NAME, updater.getFunctionName());
        assertEquals(INPUT_TOPIC_NAME, updater.getInputs());
        assertEquals(OUTPUT_TOPIC_NAME, updater.getOutput());
        assertEquals(updater.getFunctionConfig().getResources().getDisk(), Long.valueOf(8080808080808080L));
        // cpu/Ram should be default
        assertEquals(updater.getFunctionConfig().getResources().getRam(), Long.valueOf(1073741824L));
        assertEquals(updater.getFunctionConfig().getResources().getCpu(), 1.0);
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.setUpdateAuthData(true);
        verify(functions, times(1)).updateFunctionWithUrl(any(FunctionConfig.class), anyString(), eq(updateOptions));
>>>>>>> f773c602c... Test pr 10 (#27)
    }


    public static class ConsoleOutputCapturer {
        private ByteArrayOutputStream stdout;
        private ByteArrayOutputStream stderr;
        private PrintStream previous;
        private boolean capturing;

        public void start() {
            if (capturing) {
                return;
            }

            capturing = true;
            previous = System.out;
            stdout = new ByteArrayOutputStream();
            stderr = new ByteArrayOutputStream();

            OutputStream outputStreamCombinerstdout =
                    new OutputStreamCombiner(Arrays.asList(previous, stdout));
            PrintStream stdoutStream = new PrintStream(outputStreamCombinerstdout);

            OutputStream outputStreamCombinerStderr =
                    new OutputStreamCombiner(Arrays.asList(previous, stderr));
            PrintStream stderrStream = new PrintStream(outputStreamCombinerStderr);

            System.setOut(stdoutStream);
            System.setErr(stderrStream);
        }

        public void stop() {
            if (!capturing) {
                return;
            }

            System.setOut(previous);

            previous = null;
            capturing = false;
        }

        public String getStdout() {
            return stdout.toString();
        }

        public String getStderr() {
            return stderr.toString();
        }

        private static class OutputStreamCombiner extends OutputStream {
            private List<OutputStream> outputStreams;

            public OutputStreamCombiner(List<OutputStream> outputStreams) {
                this.outputStreams = outputStreams;
            }

            public void write(int b) throws IOException {
                for (OutputStream os : outputStreams) {
                    os.write(b);
                }
            }

            public void flush() throws IOException {
                for (OutputStream os : outputStreams) {
                    os.flush();
                }
            }

            public void close() throws IOException {
                for (OutputStream os : outputStreams) {
                    os.close();
                }
            }
        }
    }
}
