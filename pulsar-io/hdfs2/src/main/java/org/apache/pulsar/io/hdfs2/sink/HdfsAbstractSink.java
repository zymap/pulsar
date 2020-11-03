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
package org.apache.pulsar.io.hdfs2.sink;

import java.io.IOException;
<<<<<<< HEAD
=======
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

<<<<<<< HEAD
=======
import lombok.extern.slf4j.Slf4j;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.hdfs2.AbstractHdfsConnector;
import org.apache.pulsar.io.hdfs2.HdfsResources;

/**
 * A Simple abstract class for HDFS sink.
 * Users need to implement extractKeyValue function to use this sink.
 */
<<<<<<< HEAD
=======
@Slf4j
>>>>>>> f773c602c... Test pr 10 (#27)
public abstract class HdfsAbstractSink<K, V> extends AbstractHdfsConnector implements Sink<V> {

    protected HdfsSinkConfig hdfsSinkConfig;
    protected BlockingQueue<Record<V>> unackedRecords;
    protected HdfsSyncThread<V> syncThread;
    private Path path;
    private FSDataOutputStream hdfsStream;
<<<<<<< HEAD
=======
    private DateTimeFormatter subdirectoryFormatter;
>>>>>>> f773c602c... Test pr 10 (#27)

    public abstract KeyValue<K, V> extractKeyValue(Record<V> record);
    protected abstract void createWriter() throws IOException;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
       hdfsSinkConfig = HdfsSinkConfig.load(config);
       hdfsSinkConfig.validate();
       connectorConfig = hdfsSinkConfig;
       unackedRecords = new LinkedBlockingQueue<Record<V>> (hdfsSinkConfig.getMaxPendingRecords());
<<<<<<< HEAD
=======
       if (hdfsSinkConfig.getSubdirectoryPattern() != null) {
           subdirectoryFormatter = DateTimeFormatter.ofPattern(hdfsSinkConfig.getSubdirectoryPattern());
       }
>>>>>>> f773c602c... Test pr 10 (#27)
       connectToHdfs();
       createWriter();
       launchSyncThread();
    }

    @Override
    public void close() throws Exception {
       syncThread.halt();
       syncThread.join(0);
    }

    protected final void connectToHdfs() throws IOException {
       try {
           HdfsResources resources = hdfsResources.get();

           if (resources.getConfiguration() == null) {
               resources = this.resetHDFSResources(hdfsSinkConfig);
               hdfsResources.set(resources);
           }
       } catch (IOException ex) {
          hdfsResources.set(new HdfsResources(null, null, null));
          throw ex;
       }
    }

    protected FSDataOutputStream getHdfsStream() throws IllegalArgumentException, IOException {
        if (hdfsStream == null) {
            Path path = getPath();
            FileSystem fs = getFileSystemAsUser(getConfiguration(), getUserGroupInformation());
            hdfsStream = fs.exists(path) ? fs.append(path) : fs.create(path);
        }
        return hdfsStream;
    }

    protected final Path getPath() {
        if (path == null) {
            String ext = "";
            if (StringUtils.isNotBlank(hdfsSinkConfig.getFileExtension())) {
                ext = hdfsSinkConfig.getFileExtension();
            } else if (getCompressionCodec() != null) {
                ext = getCompressionCodec().getDefaultExtension();
            }

<<<<<<< HEAD
            path = new Path(FilenameUtils.concat(hdfsSinkConfig.getDirectory(),
                    hdfsSinkConfig.getFilenamePrefix() + "-" + System.currentTimeMillis() + ext));
=======
            String directory = hdfsSinkConfig.getDirectory();
            if (subdirectoryFormatter != null) {
                directory = FilenameUtils.concat(directory, LocalDateTime.now().format(subdirectoryFormatter));
            }
            path = new Path(FilenameUtils.concat(directory,
                    hdfsSinkConfig.getFilenamePrefix() + "-" + System.currentTimeMillis() + ext));
            log.info("Create path: {}", path);
>>>>>>> f773c602c... Test pr 10 (#27)
        }
        return path;
    }

    protected final void launchSyncThread() throws IOException {
        syncThread = new HdfsSyncThread<V>(getHdfsStream(), unackedRecords, hdfsSinkConfig.getSyncInterval());
        syncThread.start();
    }
}