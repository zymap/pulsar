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

package org.apache.pulsar.io.mongodb;

import com.mongodb.MongoBulkWriteException;
<<<<<<< HEAD
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.bulk.BulkWriteError;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.bson.BsonDocument;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
=======
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.bson.BsonDocument;
import org.bson.Document;
import org.mockito.Mock;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.testng.IObjectFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

<<<<<<< HEAD
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@PrepareForTest(MongoClients.class)
@PowerMockIgnore({"org.apache.logging.log4j.*"})
=======
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
>>>>>>> f773c602c... Test pr 10 (#27)
public class MongoSinkTest {

    @Mock
    private Record<byte[]> mockRecord;

    @Mock
    private SinkContext mockSinkContext;

    @Mock
    private MongoClient mockMongoClient;

    @Mock
    private MongoDatabase mockMongoDb;

    @Mock
    private MongoCollection mockMongoColl;

    private MongoSink sink;

    private Map<String, Object> map;

<<<<<<< HEAD
=======
    private Subscriber subscriber;

    @Mock
    private Publisher mockPublisher;
>>>>>>> f773c602c... Test pr 10 (#27)

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @BeforeMethod
    public void setUp() {
<<<<<<< HEAD
        sink = new MongoSink();
=======

>>>>>>> f773c602c... Test pr 10 (#27)
        map = TestHelper.createMap(true);

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);
        mockMongoClient = mock(MongoClient.class);
        mockMongoDb = mock(MongoDatabase.class);
        mockMongoColl = mock(MongoCollection.class);
<<<<<<< HEAD

        PowerMockito.mockStatic(MongoClients.class);

        when(MongoClients.create(anyString())).thenReturn(mockMongoClient);
        when(mockMongoClient.getDatabase(anyString())).thenReturn(mockMongoDb);
        when(mockMongoDb.getCollection(anyString())).thenReturn(mockMongoColl);
=======
        mockPublisher = mock(Publisher.class);
        sink = new MongoSink(() -> mockMongoClient);


        when(mockMongoClient.getDatabase(anyString())).thenReturn(mockMongoDb);
        when(mockMongoDb.getCollection(anyString())).thenReturn(mockMongoColl);
        when(mockMongoDb.getCollection(anyString()).insertMany(any())).thenReturn(mockPublisher);
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void initContext(boolean throwBulkError) {
        when(mockRecord.getValue()).thenReturn("{\"hello\":\"pulsar\"}".getBytes());

        doAnswer((invocation) -> {
<<<<<<< HEAD
            SingleResultCallback cb = invocation.getArgumentAt(1, SingleResultCallback.class);
            MongoBulkWriteException exc = null;

            if (throwBulkError) {
                List<BulkWriteError > writeErrors = Arrays.asList(
                        new BulkWriteError(0, "error", new BsonDocument(), 1));
                exc = new MongoBulkWriteException(null, writeErrors, null, null);
            }

            cb.onResult(null, exc);
            return null;
        }).when(mockMongoColl).insertMany(anyObject(), anyObject());
=======
            subscriber = invocation.getArgument(0,Subscriber.class);
            MongoBulkWriteException exc = null;
            if (throwBulkError) {
                List<BulkWriteError> writeErrors = Arrays.asList(
                        new BulkWriteError(0, "error", new BsonDocument(), 1));
                exc = new MongoBulkWriteException(null, writeErrors, null, null);
            }
            subscriber.onError(exc);
            return null;
        }).when(mockPublisher).subscribe(any());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    private void initFailContext(String msg) {
        when(mockRecord.getValue()).thenReturn(msg.getBytes());

        doAnswer((invocation) -> {
<<<<<<< HEAD
            SingleResultCallback cb = invocation.getArgumentAt(1, SingleResultCallback.class);
            cb.onResult(null, new Exception("Oops"));
            return null;
        }).when(mockMongoColl).insertMany(anyObject(), anyObject());
=======
            subscriber = invocation.getArgument(0, Subscriber.class);
            subscriber.onError(new Exception("0ops"));
            return null;
        }).when(mockPublisher).subscribe(any());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @AfterMethod
    public void tearDown() throws Exception {
        sink.close();
        verify(mockMongoClient, times(1)).close();
    }

    @Test
    public void testOpen() throws Exception {
        sink.open(map, mockSinkContext);
    }

    @Test
    public void testWriteNullMessage() throws Exception {
        when(mockRecord.getValue()).thenReturn("".getBytes());

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteGoodMessage() throws Exception {
        initContext(false);

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).ack();
    }

    @Test
    public void testWriteMultipleMessages() throws Exception {
        initContext(true);

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);
        sink.write(mockRecord);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(2)).ack();
        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteWithError() throws Exception {
        initFailContext("{\"hello\":\"pulsar\"}");

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }

    @Test
    public void testWriteBadMessage() throws Exception {
        initFailContext("Oops");

        sink.open(map, mockSinkContext);
        sink.write(mockRecord);

        Thread.sleep(1000);

        verify(mockRecord, times(1)).fail();
    }
}
