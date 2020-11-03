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
package org.apache.pulsar.sql.presto;

<<<<<<< HEAD
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.javax.ws.rs.ClientErrorException;
import org.apache.pulsar.shade.javax.ws.rs.core.Response;
import org.testng.Assert;
=======
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;

import org.apache.pulsar.common.schema.SchemaType;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true)
=======
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

>>>>>>> f773c602c... Test pr 10 (#27)
public class TestPulsarMetadata extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarMetadata.class);

<<<<<<< HEAD
    @Test
    public void testListSchemaNames() {

        List<String> schemas = this.pulsarMetadata.listSchemaNames(mock(ConnectorSession.class));

        String[] expectedSchemas = {NAMESPACE_NAME_1.toString(), NAMESPACE_NAME_2.toString(),
                NAMESPACE_NAME_3.toString(), NAMESPACE_NAME_4.toString()};
        Assert.assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
    }

    @Test
    public void testGetTableHandle() {

=======
    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListSchemaNames(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<String> schemas = this.pulsarMetadata.listSchemaNames(mock(ConnectorSession.class));


        if (StringUtils.isBlank(delimiter)) {
            String[] expectedSchemas = {NAMESPACE_NAME_1.toString(), NAMESPACE_NAME_2.toString(),
                    NAMESPACE_NAME_3.toString(), NAMESPACE_NAME_4.toString()};
            assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
        } else {
            String[] expectedSchemas = {
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_1.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_2.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_3.toString(), pulsarConnectorConfig),
                    PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded(NAMESPACE_NAME_4.toString(), pulsarConnectorConfig)};
            assertEquals(new HashSet<>(schemas), new HashSet<>(Arrays.asList(expectedSchemas)));
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableHandle(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
>>>>>>> f773c602c... Test pr 10 (#27)
        SchemaTableName schemaTableName = new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName());

        ConnectorTableHandle connectorTableHandle
                = this.pulsarMetadata.getTableHandle(mock(ConnectorSession.class), schemaTableName);

<<<<<<< HEAD
        Assert.assertTrue(connectorTableHandle instanceof PulsarTableHandle);

        PulsarTableHandle pulsarTableHandle = (PulsarTableHandle) connectorTableHandle;

        Assert.assertEquals(pulsarTableHandle.getConnectorId(), pulsarConnectorId.toString());
        Assert.assertEquals(pulsarTableHandle.getSchemaName(), TOPIC_1.getNamespace());
        Assert.assertEquals(pulsarTableHandle.getTableName(), TOPIC_1.getLocalName());
        Assert.assertEquals(pulsarTableHandle.getTopicName(), TOPIC_1.getLocalName());
    }

    @Test
    public void testGetTableMetadata() {

        List<TopicName> allTopics = new LinkedList<>();
        allTopics.addAll(topicNames);
=======
        assertTrue(connectorTableHandle instanceof PulsarTableHandle);

        PulsarTableHandle pulsarTableHandle = (PulsarTableHandle) connectorTableHandle;

        assertEquals(pulsarTableHandle.getConnectorId(), pulsarConnectorId.toString());
        assertEquals(pulsarTableHandle.getSchemaName(), TOPIC_1.getNamespace());
        assertEquals(pulsarTableHandle.getTableName(), TOPIC_1.getLocalName());
        assertEquals(pulsarTableHandle.getTopicName(), TOPIC_1.getLocalName());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadata(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<TopicName> allTopics = new LinkedList<>();
        allTopics.addAll(topicNames.stream().filter(topicName -> !topicName.equals(NON_SCHEMA_TOPIC)).collect(Collectors.toList()));
>>>>>>> f773c602c... Test pr 10 (#27)
        allTopics.addAll(partitionedTopicNames);

        for (TopicName topic : allTopics) {
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                    topic.toString(),
                    topic.getNamespace(),
                    topic.getLocalName(),
                    topic.getLocalName()
            );

            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);

<<<<<<< HEAD
            Assert.assertEquals(tableMetadata.getTable().getSchemaName(), topic.getNamespace());
            Assert.assertEquals(tableMetadata.getTable().getTableName(), topic.getLocalName());

            Assert.assertEquals(tableMetadata.getColumns().size(),
=======
            assertEquals(tableMetadata.getTable().getSchemaName(), topic.getNamespace());
            assertEquals(tableMetadata.getTable().getTableName(), topic.getLocalName());
            assertEquals(tableMetadata.getColumns().size(),
>>>>>>> f773c602c... Test pr 10 (#27)
                    fooColumnHandles.size());

            List<String> fieldNames = new LinkedList<>(fooFieldNames.keySet());

            for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
                fieldNames.add(internalField.getName());
            }

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
<<<<<<< HEAD
                    Assert.assertEquals(column.getComment(),
=======
                    assertEquals(column.getComment(),
>>>>>>> f773c602c... Test pr 10 (#27)
                            PulsarInternalColumn.getInternalFieldsMap()
                                    .get(column.getName()).getColumnMetadata(true).getComment());
                }

                fieldNames.remove(column.getName());
            }

<<<<<<< HEAD
            Assert.assertTrue(fieldNames.isEmpty());
        }
    }

    @Test
    public void testGetTableMetadataWrongSchema() {

=======
            assertTrue(fieldNames.isEmpty());
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataWrongSchema(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
>>>>>>> f773c602c... Test pr 10 (#27)
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                "wrong-tenant/wrong-ns",
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
<<<<<<< HEAD
            Assert.fail("Invalid schema should have generated an exception");
        } catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            Assert.assertEquals(e.getMessage(), "Schema wrong-tenant/wrong-ns does not exist");
        }
    }

    @Test
    public void testGetTableMetadataWrongTable() {

=======
            fail("Invalid schema should have generated an exception");
        } catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(e.getMessage(), "Schema wrong-tenant/wrong-ns does not exist");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataWrongTable(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
>>>>>>> f773c602c... Test pr 10 (#27)
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                "wrong-topic",
                "wrong-topic"
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
<<<<<<< HEAD
            Assert.fail("Invalid table should have generated an exception");
        } catch (TableNotFoundException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            Assert.assertEquals(e.getMessage(), "Table 'tenant-1/ns-1.wrong-topic' not found");
        }
    }

    @Test
    public void testGetTableMetadataTableNoSchema() throws PulsarAdminException {

=======
            fail("Invalid table should have generated an exception");
        } catch (TableNotFoundException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            assertEquals(e.getMessage(), "Table 'tenant-1/ns-1.wrong-topic' not found");
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableNoSchema(String delimiter) throws PulsarAdminException {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
>>>>>>> f773c602c... Test pr 10 (#27)
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenThrow(
                new PulsarAdminException(new ClientErrorException(Response.Status.NOT_FOUND)));

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );


        ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                pulsarTableHandle);
<<<<<<< HEAD
        Assert.assertEquals(tableMetadata.getColumns().size(), 0);
    }

    @Test
    public void testGetTableMetadataTableBlankSchema() throws PulsarAdminException {

        SchemaInfo badSchemaInfo = new SchemaInfo();
        badSchemaInfo.setSchema(new byte[0]);
=======
        assertEquals(tableMetadata.getColumns().size(), PulsarInternalColumn.getInternalFields().size() + 1);
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableBlankSchema(String delimiter) throws PulsarAdminException {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaInfo badSchemaInfo = new SchemaInfo();
        badSchemaInfo.setSchema(new byte[0]);
        badSchemaInfo.setType(SchemaType.AVRO);
>>>>>>> f773c602c... Test pr 10 (#27)
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenReturn(badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
<<<<<<< HEAD
            Assert.fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            Assert.assertEquals(e.getMessage(),
=======
            fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(),
>>>>>>> f773c602c... Test pr 10 (#27)
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
    }

<<<<<<< HEAD
    @Test
    public void testGetTableMetadataTableInvalidSchema() throws PulsarAdminException {

        SchemaInfo badSchemaInfo = new SchemaInfo();
        badSchemaInfo.setSchema("foo".getBytes());
=======
    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetTableMetadataTableInvalidSchema(String delimiter) throws PulsarAdminException {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        SchemaInfo badSchemaInfo = new SchemaInfo();
        badSchemaInfo.setSchema("foo".getBytes());
        badSchemaInfo.setType(SchemaType.AVRO);
>>>>>>> f773c602c... Test pr 10 (#27)
        when(this.schemas.getSchemaInfo(eq(TOPIC_1.getSchemaName()))).thenReturn(badSchemaInfo);

        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(
                pulsarConnectorId.toString(),
                TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(),
                TOPIC_1.getLocalName()
        );

        try {
            ConnectorTableMetadata tableMetadata = this.pulsarMetadata.getTableMetadata(mock(ConnectorSession.class),
                    pulsarTableHandle);
<<<<<<< HEAD
            Assert.fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            Assert.assertEquals(e.getMessage(),
=======
            fail("Table without schema should have generated an exception");
        } catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(),
>>>>>>> f773c602c... Test pr 10 (#27)
                    "Topic persistent://tenant-1/ns-1/topic-1 does not have a valid schema");
        }
    }

<<<<<<< HEAD
    @Test
    public void testListTable() {
        Assert.assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), null).isEmpty());
        Assert.assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), "wrong-tenant/wrong-ns")
                .isEmpty());

        SchemaTableName[] expectedTopics1 = {new SchemaTableName(TOPIC_4.getNamespace(), TOPIC_4.getLocalName())};
        Assert.assertEquals(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                NAMESPACE_NAME_3.toString()), Arrays.asList(expectedTopics1));

        SchemaTableName[] expectedTopics2 = {new SchemaTableName(TOPIC_5.getNamespace(), TOPIC_5.getLocalName()),
                new SchemaTableName(TOPIC_6.getNamespace(), TOPIC_6.getLocalName())};
        Assert.assertEquals(new HashSet<>(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                NAMESPACE_NAME_4.toString())), new HashSet<>(Arrays.asList(expectedTopics2)));
    }

    @Test
    public void testGetColumnHandles() {

=======
    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListTable(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), Optional.empty()).isEmpty());
        assertTrue(this.pulsarMetadata.listTables(mock(ConnectorSession.class), Optional.of("wrong-tenant/wrong-ns"))
                .isEmpty());

        SchemaTableName[] expectedTopics1 = {new SchemaTableName(
            TOPIC_4.getNamespace(), TOPIC_4.getLocalName()),
            new SchemaTableName(PARTITIONED_TOPIC_4.getNamespace(), PARTITIONED_TOPIC_4.getLocalName())
        };
        assertEquals(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
                Optional.of(NAMESPACE_NAME_3.toString())), Arrays.asList(expectedTopics1));

        SchemaTableName[] expectedTopics2 = {new SchemaTableName(TOPIC_5.getNamespace(), TOPIC_5.getLocalName()),
                new SchemaTableName(TOPIC_6.getNamespace(), TOPIC_6.getLocalName()),
            new SchemaTableName(PARTITIONED_TOPIC_5.getNamespace(), PARTITIONED_TOPIC_5.getLocalName()),
            new SchemaTableName(PARTITIONED_TOPIC_6.getNamespace(), PARTITIONED_TOPIC_6.getLocalName()),
        };
        assertEquals(new HashSet<>(this.pulsarMetadata.listTables(mock(ConnectorSession.class),
            Optional.of(NAMESPACE_NAME_4.toString()))), new HashSet<>(Arrays.asList(expectedTopics2)));
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetColumnHandles(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
>>>>>>> f773c602c... Test pr 10 (#27)
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(), TOPIC_1.getNamespace(),
                TOPIC_1.getLocalName(), TOPIC_1.getLocalName());
        Map<String, ColumnHandle> columnHandleMap
                = new HashMap<>(this.pulsarMetadata.getColumnHandles(mock(ConnectorSession.class), pulsarTableHandle));

        List<String> fieldNames = new LinkedList<>(fooFieldNames.keySet());

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (String field : fieldNames) {
<<<<<<< HEAD
            Assert.assertNotNull(columnHandleMap.get(field));
            PulsarColumnHandle pulsarColumnHandle = (PulsarColumnHandle) columnHandleMap.get(field);
            PulsarInternalColumn pulsarInternalColumn = PulsarInternalColumn.getInternalFieldsMap().get(field);
            if (pulsarInternalColumn != null) {
                Assert.assertEquals(pulsarColumnHandle,
=======
            assertNotNull(columnHandleMap.get(field));
            PulsarColumnHandle pulsarColumnHandle = (PulsarColumnHandle) columnHandleMap.get(field);
            PulsarInternalColumn pulsarInternalColumn = PulsarInternalColumn.getInternalFieldsMap().get(field);
            if (pulsarInternalColumn != null) {
                assertEquals(pulsarColumnHandle,
>>>>>>> f773c602c... Test pr 10 (#27)
                        pulsarInternalColumn.getColumnHandle(pulsarConnectorId.toString(), false));
            } else {
                Schema schema = new Schema.Parser().parse(new String(topicsToSchemas.get(TOPIC_1.getSchemaName())
                        .getSchema()));
<<<<<<< HEAD
                Assert.assertEquals(pulsarColumnHandle.getConnectorId(), pulsarConnectorId.toString());
                Assert.assertEquals(pulsarColumnHandle.getName(), field);
                Assert.assertEquals(pulsarColumnHandle.getPositionIndices(), fooPositionIndices.get(field));
                Assert.assertEquals(pulsarColumnHandle.getFieldNames(), fooFieldNames.get(field));
                Assert.assertEquals(pulsarColumnHandle.getType(), fooTypes.get(field));
                Assert.assertEquals(pulsarColumnHandle.isHidden(), false);
            }
            columnHandleMap.remove(field);
        }
        Assert.assertTrue(columnHandleMap.isEmpty());
    }

    @Test
    public void testListTableColumns() {
=======
                assertEquals(pulsarColumnHandle.getConnectorId(), pulsarConnectorId.toString());
                assertEquals(pulsarColumnHandle.getName(), field);
                assertEquals(pulsarColumnHandle.getPositionIndices(), fooPositionIndices.get(field));
                assertEquals(pulsarColumnHandle.getFieldNames(), fooFieldNames.get(field));
                assertEquals(pulsarColumnHandle.getType(), fooTypes.get(field));
                assertFalse(pulsarColumnHandle.isHidden());
            }
            columnHandleMap.remove(field);
        }
        assertTrue(columnHandleMap.isEmpty());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testListTableColumns(String delimiter) {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
>>>>>>> f773c602c... Test pr 10 (#27)
        Map<SchemaTableName, List<ColumnMetadata>> tableColumnsMap
                = this.pulsarMetadata.listTableColumns(mock(ConnectorSession.class),
                new SchemaTablePrefix(TOPIC_1.getNamespace()));

<<<<<<< HEAD
        Assert.assertEquals(tableColumnsMap.size(), 2);
        List<ColumnMetadata> columnMetadataList
                = tableColumnsMap.get(new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName()));
        Assert.assertNotNull(columnMetadataList);
        Assert.assertEquals(columnMetadataList.size(),
=======
        assertEquals(tableColumnsMap.size(), 4);
        List<ColumnMetadata> columnMetadataList
                = tableColumnsMap.get(new SchemaTableName(TOPIC_1.getNamespace(), TOPIC_1.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
>>>>>>> f773c602c... Test pr 10 (#27)
                fooColumnHandles.size());

        List<String> fieldNames = new LinkedList<>(fooFieldNames.keySet());

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
<<<<<<< HEAD
                Assert.assertEquals(column.getComment(),
=======
                assertEquals(column.getComment(),
>>>>>>> f773c602c... Test pr 10 (#27)
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

<<<<<<< HEAD
        Assert.assertTrue(fieldNames.isEmpty());

        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_2.getNamespace(), TOPIC_2.getLocalName()));
        Assert.assertNotNull(columnMetadataList);
        Assert.assertEquals(columnMetadataList.size(),
=======
        assertTrue(fieldNames.isEmpty());

        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_2.getNamespace(), TOPIC_2.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
>>>>>>> f773c602c... Test pr 10 (#27)
                fooColumnHandles.size());

        fieldNames = new LinkedList<>(fooFieldNames.keySet());

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
<<<<<<< HEAD
                Assert.assertEquals(column.getComment(),
=======
                assertEquals(column.getComment(),
>>>>>>> f773c602c... Test pr 10 (#27)
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

<<<<<<< HEAD
        Assert.assertTrue(fieldNames.isEmpty());
=======
        assertTrue(fieldNames.isEmpty());
>>>>>>> f773c602c... Test pr 10 (#27)

        // test table and schema
        tableColumnsMap
                = this.pulsarMetadata.listTableColumns(mock(ConnectorSession.class),
                new SchemaTablePrefix(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));

<<<<<<< HEAD
        Assert.assertEquals(tableColumnsMap.size(), 1);
        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));
        Assert.assertNotNull(columnMetadataList);
        Assert.assertEquals(columnMetadataList.size(),
=======
        assertEquals(tableColumnsMap.size(), 1);
        columnMetadataList = tableColumnsMap.get(new SchemaTableName(TOPIC_4.getNamespace(), TOPIC_4.getLocalName()));
        assertNotNull(columnMetadataList);
        assertEquals(columnMetadataList.size(),
>>>>>>> f773c602c... Test pr 10 (#27)
                fooColumnHandles.size());

        fieldNames = new LinkedList<>(fooFieldNames.keySet());

        for (PulsarInternalColumn internalField : PulsarInternalColumn.getInternalFields()) {
            fieldNames.add(internalField.getName());
        }

        for (ColumnMetadata column : columnMetadataList) {
            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(column.getName())) {
<<<<<<< HEAD
                Assert.assertEquals(column.getComment(),
=======
                assertEquals(column.getComment(),
>>>>>>> f773c602c... Test pr 10 (#27)
                        PulsarInternalColumn.getInternalFieldsMap()
                                .get(column.getName()).getColumnMetadata(true).getComment());
            }

            fieldNames.remove(column.getName());
        }

<<<<<<< HEAD
        Assert.assertTrue(fieldNames.isEmpty());
=======
        assertTrue(fieldNames.isEmpty());
>>>>>>> f773c602c... Test pr 10 (#27)
    }
}
