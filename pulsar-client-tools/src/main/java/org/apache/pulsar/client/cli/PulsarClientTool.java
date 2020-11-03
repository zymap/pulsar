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
package org.apache.pulsar.client.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.FileInputStream;
<<<<<<< HEAD
import java.net.MalformedURLException;
=======
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
<<<<<<< HEAD
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

=======
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.IUsageFormatter;
>>>>>>> f773c602c... Test pr 10 (#27)
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "Produce or consume messages on a specified topic")
public class PulsarClientTool {

    @Parameter(names = { "--url" }, description = "Broker URL to which to connect.")
    String serviceURL = null;

<<<<<<< HEAD
    @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
    String authPluginClassName = null;

    @Parameter(names = { "--auth-params" }, description = "Authentication parameters, e.g., \"key1:val1,key2:val2\".")
=======
    @Parameter(names = { "--proxy-url" }, description = "Proxy-server URL to which to connect.")
    String proxyServiceURL = null;

    @Parameter(names = { "--proxy-protocol" }, description = "Proxy protocol to select type of routing at proxy.")
    ProxyProtocol proxyProtocol = null;

    @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
    String authPluginClassName = null;

    @Parameter(names = { "--listener-name" }, description = "Listener name for the broker.")
    String listenerName = null;

    @Parameter(
        names = { "--auth-params" },
        description = "Authentication parameters, whose format is determined by the implementation " +
            "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" " +
            "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
>>>>>>> f773c602c... Test pr 10 (#27)
    String authParams = null;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    boolean tlsAllowInsecureConnection = false;
    boolean tlsEnableHostnameVerification = false;
    String tlsTrustCertsFilePath = null;

<<<<<<< HEAD
    JCommander commandParser;
    CmdProduce produceCommand;
    CmdConsume consumeCommand;

    public PulsarClientTool(Properties properties) throws MalformedURLException {
=======
    // for tls with keystore type config
    boolean useKeyStoreTls = false;
    String tlsTrustStoreType = "JKS";
    String tlsTrustStorePath = null;
    String tlsTrustStorePassword = null;

    JCommander commandParser;
    IUsageFormatter usageFormatter;
    CmdProduce produceCommand;
    CmdConsume consumeCommand;

    public PulsarClientTool(Properties properties) {
>>>>>>> f773c602c... Test pr 10 (#27)
        this.serviceURL = StringUtils.isNotBlank(properties.getProperty("brokerServiceUrl"))
                ? properties.getProperty("brokerServiceUrl") : properties.getProperty("webServiceUrl");
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        if (StringUtils.isBlank(this.serviceURL)) {
            this.serviceURL = properties.getProperty("serviceUrl");
        }
        this.authPluginClassName = properties.getProperty("authPlugin");
        this.authParams = properties.getProperty("authParams");
        this.tlsAllowInsecureConnection = Boolean
                .parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));
        this.tlsEnableHostnameVerification = Boolean
                .parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        this.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");

<<<<<<< HEAD
=======
        this.useKeyStoreTls = Boolean
                .parseBoolean(properties.getProperty("useKeyStoreTls", "false"));
        this.tlsTrustStoreType = properties.getProperty("tlsTrustStoreType", "JKS");
        this.tlsTrustStorePath = properties.getProperty("tlsTrustStorePath");
        this.tlsTrustStorePassword = properties.getProperty("tlsTrustStorePassword");

>>>>>>> f773c602c... Test pr 10 (#27)
        produceCommand = new CmdProduce();
        consumeCommand = new CmdConsume();

        this.commandParser = new JCommander();
<<<<<<< HEAD
=======
        this.usageFormatter = new DefaultUsageFormatter(this.commandParser);

>>>>>>> f773c602c... Test pr 10 (#27)
        commandParser.setProgramName("pulsar-client");
        commandParser.addObject(this);
        commandParser.addCommand("produce", produceCommand);
        commandParser.addCommand("consume", consumeCommand);
    }

<<<<<<< HEAD
    private void updateConfig() throws UnsupportedAuthenticationException, MalformedURLException {
        ClientBuilder clientBuilder = PulsarClient.builder();
        if (isNotBlank(this.authPluginClassName)) {
            clientBuilder.authentication(authPluginClassName, authParams);
        }
        clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        clientBuilder.tlsTrustCertsFilePath(this.tlsTrustCertsFilePath);
        clientBuilder.serviceUrl(serviceURL);
        this.produceCommand.updateConfig(clientBuilder);
        this.consumeCommand.updateConfig(clientBuilder);
=======
    private void updateConfig() throws UnsupportedAuthenticationException {
        ClientBuilder clientBuilder = PulsarClient.builder();
        Authentication authentication = null;
        if (isNotBlank(this.authPluginClassName)) {
            authentication = AuthenticationFactory.create(authPluginClassName, authParams);
            clientBuilder.authentication(authentication);
        }
        if (isNotBlank(this.listenerName)) {
            clientBuilder.listenerName(this.listenerName);
        }
        clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        clientBuilder.tlsTrustCertsFilePath(this.tlsTrustCertsFilePath);
        clientBuilder.enableTlsHostnameVerification(this.tlsEnableHostnameVerification);
        clientBuilder.serviceUrl(serviceURL);

        clientBuilder.useKeyStoreTls(useKeyStoreTls)
                .tlsTrustStoreType(tlsTrustStoreType)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(tlsTrustStorePassword);

        if (StringUtils.isNotBlank(proxyServiceURL)) {
            if (proxyProtocol == null) {
                System.out.println("proxy-protocol must be provided with proxy-url");
                System.exit(-1);
            }
            clientBuilder.proxyServiceUrl(proxyServiceURL, proxyProtocol);
        }
        this.produceCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
        this.consumeCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    public int run(String[] args) {
        try {
            commandParser.parse(args);

            if (isBlank(this.serviceURL)) {
                commandParser.usage();
                return -1;
            }

            if (help) {
                commandParser.usage();
                return 0;
            }

            try {
                this.updateConfig(); // If the --url, --auth-plugin, or --auth-params parameter are not specified,
                                     // it will default to the values passed in by the constructor
<<<<<<< HEAD
            } catch (MalformedURLException mue) {
                System.out.println("Unable to parse URL " + this.serviceURL);
                commandParser.usage();
                return -1;
            } catch (UnsupportedAuthenticationException exp) {
                System.out.println("Failed to load an authentication plugin");
                commandParser.usage();
=======
            } catch (UnsupportedAuthenticationException exp) {
                System.out.println("Failed to load an authentication plugin");
                exp.printStackTrace();
>>>>>>> f773c602c... Test pr 10 (#27)
                return -1;
            }

            String chosenCommand = commandParser.getParsedCommand();
            if ("produce".equals(chosenCommand)) {
                return produceCommand.run();
            } else if ("consume".equals(chosenCommand)) {
                return consumeCommand.run();
            } else {
                commandParser.usage();
                return -1;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            String chosenCommand = commandParser.getParsedCommand();
            if (e instanceof ParameterException) {
<<<<<<< HEAD
                commandParser.usage(chosenCommand);
=======
                usageFormatter.usage(chosenCommand);
>>>>>>> f773c602c... Test pr 10 (#27)
            } else {
                e.printStackTrace();
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: pulsar-client CONF_FILE_PATH [options] [command] [command options]");
            System.exit(-1);
        }
        String configFile = args[0];
        Properties properties = new Properties();

        if (configFile != null) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }
        }

        PulsarClientTool clientTool = new PulsarClientTool(properties);
        int exit_code = clientTool.run(Arrays.copyOfRange(args, 1, args.length));

        System.exit(exit_code);

    }
}
