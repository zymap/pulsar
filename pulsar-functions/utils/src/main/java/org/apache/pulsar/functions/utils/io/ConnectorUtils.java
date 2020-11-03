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
package org.apache.pulsar.functions.utils.io;

import java.io.File;
import java.io.IOException;
<<<<<<< HEAD
=======
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
<<<<<<< HEAD
import java.util.Collections;
=======
import java.util.*;
>>>>>>> f773c602c... Test pr 10 (#27)

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
<<<<<<< HEAD
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
=======
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.annotations.FieldDoc;
>>>>>>> f773c602c... Test pr 10 (#27)

@UtilityClass
@Slf4j
public class ConnectorUtils {

    private static final String PULSAR_IO_SERVICE_NAME = "pulsar-io.yaml";

    /**
     * Extract the Pulsar IO Source class from a connector archive.
     */
    public static String getIOSourceClass(NarClassLoader ncl) throws IOException {
        String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        ConnectorDefinition conf = ObjectMapperFactory.getThreadLocalYaml().readValue(configStr,
                ConnectorDefinition.class);
        if (StringUtils.isEmpty(conf.getSourceClass())) {
            throw new IOException(
                    String.format("The '%s' connector does not provide a source implementation", conf.getName()));
        }

        try {
            // Try to load source class and check it implements Source interface
<<<<<<< HEAD
            Object instance = ncl.loadClass(conf.getSourceClass()).newInstance();
            if (!(instance instanceof Source)) {
                throw new IOException("Class " + conf.getSourceClass() + " does not implement interface "
                        + Source.class.getName());
=======
            Class sourceClass = ncl.loadClass(conf.getSourceClass());
            if (!(Source.class.isAssignableFrom(sourceClass) || BatchSource.class.isAssignableFrom(sourceClass))) {
                throw new IOException(String.format("Class %s does not implement interface %s or %s",
                  conf.getSourceClass(), Source.class.getName(), BatchSource.class.getName()));
>>>>>>> f773c602c... Test pr 10 (#27)
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getSourceClass();
    }

    /**
     * Extract the Pulsar IO Sink class from a connector archive.
     */
    public static String getIOSinkClass(ClassLoader classLoader) throws IOException {
<<<<<<< HEAD
        NarClassLoader ncl = (NarClassLoader) classLoader;
        String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        ConnectorDefinition conf = ObjectMapperFactory.getThreadLocalYaml().readValue(configStr,
                ConnectorDefinition.class);
=======
        ConnectorDefinition conf = getConnectorDefinition(classLoader);
        NarClassLoader ncl = (NarClassLoader) classLoader;
>>>>>>> f773c602c... Test pr 10 (#27)
        if (StringUtils.isEmpty(conf.getSinkClass())) {
            throw new IOException(
                    String.format("The '%s' connector does not provide a sink implementation", conf.getName()));
        }

        try {
            // Try to load source class and check it implements Sink interface
<<<<<<< HEAD
            Object instance = ncl.loadClass(conf.getSinkClass()).newInstance();
            if (!(instance instanceof Sink)) {
=======
            Class sinkClass = ncl.loadClass(conf.getSinkClass());
            if (!(Sink.class.isAssignableFrom(sinkClass))) {
>>>>>>> f773c602c... Test pr 10 (#27)
                throw new IOException(
                        "Class " + conf.getSinkClass() + " does not implement interface " + Sink.class.getName());
            }
        } catch (Throwable t) {
            Exceptions.rethrowIOException(t);
        }

        return conf.getSinkClass();
    }

<<<<<<< HEAD
    public static ConnectorDefinition getConnectorDefinition(String narPath) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet())) {
            String configStr = ncl.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

            return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, ConnectorDefinition.class);
        }
    }

    public static Connectors searchForConnectors(String connectorsDirectory) throws IOException {
=======
    public static ConnectorDefinition getConnectorDefinition(String narPath, String narExtractionDirectory) throws IOException {
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet(), narExtractionDirectory)) {
            return getConnectorDefinition(ncl);
        }
    }

    public static ConnectorDefinition getConnectorDefinition(ClassLoader classLoader) throws IOException {
        NarClassLoader narClassLoader = (NarClassLoader) classLoader;
        String configStr = narClassLoader.getServiceDefinition(PULSAR_IO_SERVICE_NAME);

        return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, ConnectorDefinition.class);
    }

    public static List<ConfigFieldDefinition> getConnectorConfigDefinition(String narPath,
                                                                           String configClassName,
                                                                           String narExtractionDirectory) throws Exception {
        List<ConfigFieldDefinition> retval = new LinkedList<>();
        try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(narPath), Collections.emptySet(), narExtractionDirectory)) {
            Class configClass = ncl.loadClass(configClassName);
            for (Field field : Reflections.getAllFields(configClass)) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    // We dont want static fields
                    continue;
                }
                field.setAccessible(true);
                ConfigFieldDefinition configFieldDefinition = new ConfigFieldDefinition();
                configFieldDefinition.setFieldName(field.getName());
                configFieldDefinition.setTypeName(field.getType().getName());
                Map<String, String> attributes = new HashMap<>();
                for (Annotation annotation : field.getAnnotations()) {
                    if (annotation.annotationType().equals(FieldDoc.class)) {
                        FieldDoc fieldDoc = (FieldDoc) annotation;
                        for (Method method : FieldDoc.class.getDeclaredMethods()) {
                            Object value = method.invoke(fieldDoc);
                            attributes.put(method.getName(), value == null ? "" : value.toString());
                        }
                    }
                }
                configFieldDefinition.setAttributes(attributes);
                retval.add(configFieldDefinition);
            }
        }
        return retval;
    }

    public static Connectors searchForConnectors(String connectorsDirectory, String narExtractionDirectory) throws IOException {
>>>>>>> f773c602c... Test pr 10 (#27)
        Path path = Paths.get(connectorsDirectory).toAbsolutePath();
        log.info("Searching for connectors in {}", path);

        Connectors connectors = new Connectors();

        if (!path.toFile().exists()) {
            log.warn("Connectors archive directory not found");
            return connectors;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
<<<<<<< HEAD
                    ConnectorDefinition cntDef = ConnectorUtils.getConnectorDefinition(archive.toString());
=======
                    ConnectorDefinition cntDef = ConnectorUtils.getConnectorDefinition(archive.toString(), narExtractionDirectory);
>>>>>>> f773c602c... Test pr 10 (#27)
                    log.info("Found connector {} from {}", cntDef, archive);

                    if (!StringUtils.isEmpty(cntDef.getSourceClass())) {
                        connectors.sources.put(cntDef.getName(), archive);
<<<<<<< HEAD
=======
                        if (!StringUtils.isEmpty(cntDef.getSourceConfigClass())) {
                            connectors.sourceConfigDefinitions.put(cntDef.getName(), getConnectorConfigDefinition(archive.toString(), cntDef.getSourceConfigClass(), narExtractionDirectory));
                        }
>>>>>>> f773c602c... Test pr 10 (#27)
                    }

                    if (!StringUtils.isEmpty(cntDef.getSinkClass())) {
                        connectors.sinks.put(cntDef.getName(), archive);
<<<<<<< HEAD
=======
                        if (!StringUtils.isEmpty(cntDef.getSinkConfigClass())) {
                            connectors.sinkConfigDefinitions.put(cntDef.getName(), getConnectorConfigDefinition(archive.toString(), cntDef.getSinkConfigClass(), narExtractionDirectory));
                        }
>>>>>>> f773c602c... Test pr 10 (#27)
                    }

                    connectors.connectors.add(cntDef);
                } catch (Throwable t) {
                    log.warn("Failed to load connector from {}", archive, t);
                }
            }
        }

        Collections.sort(connectors.connectors,
                (c1, c2) -> String.CASE_INSENSITIVE_ORDER.compare(c1.getName(), c2.getName()));

        return connectors;
    }
}
