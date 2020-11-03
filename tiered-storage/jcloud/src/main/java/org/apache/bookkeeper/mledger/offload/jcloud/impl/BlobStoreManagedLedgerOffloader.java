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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

<<<<<<< HEAD
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
=======
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
>>>>>>> f773c602c... Test pr 10 (#27)
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
<<<<<<< HEAD
import lombok.Data;
=======

import lombok.extern.slf4j.Slf4j;

>>>>>>> f773c602c... Test pr 10 (#27)
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
<<<<<<< HEAD
import org.apache.bookkeeper.mledger.offload.jcloud.TieredStorageConfigurationData;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
=======
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.BlobStoreLocation;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.jclouds.blobstore.BlobStore;
>>>>>>> f773c602c... Test pr 10 (#27)
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
<<<<<<< HEAD
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.googlecloudstorage.GoogleCloudStorageProviderMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.s3.reference.S3Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreManagedLedgerOffloader implements LedgerOffloader {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreManagedLedgerOffloader.class);

    private static final String METADATA_FIELD_BUCKET = "bucket";
    private static final String METADATA_FIELD_REGION = "region";
    private static final String METADATA_FIELD_ENDPOINT = "endpoint";

    public static final String[] DRIVER_NAMES = {"S3", "aws-s3", "google-cloud-storage"};

    // use these keys for both s3 and gcs.
    static final String METADATA_FORMAT_VERSION_KEY = "S3ManagedLedgerOffloaderFormatVersion";
    static final String CURRENT_VERSION = String.valueOf(1);

    public static boolean driverSupported(String driver) {
        return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(driver));
    }

    public static boolean isS3Driver(String driver) {
        return driver.equalsIgnoreCase(DRIVER_NAMES[0]) || driver.equalsIgnoreCase(DRIVER_NAMES[1]);
    }

    public static boolean isGcsDriver(String driver) {
        return driver.equalsIgnoreCase(DRIVER_NAMES[2]);
    }

    private static void addVersionInfo(BlobBuilder blobBuilder, Map<String, String> userMetadata) {
        ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
        metadataBuilder.putAll(userMetadata);
        metadataBuilder.put(METADATA_FORMAT_VERSION_KEY.toLowerCase(), CURRENT_VERSION);
        blobBuilder.userMetadata(metadataBuilder.build());
    }

    @Data(staticConstructor = "of")
    private static class BlobStoreLocation {
        private final String region;
        private final String endpoint;
    }

    private static Pair<BlobStoreLocation, BlobStore> createBlobStore(String driver,
                                                                      String region,
                                                                      String endpoint,
                                                                      Credentials credentials,
                                                                      int maxBlockSize) {
        Properties overrides = new Properties();
        // This property controls the number of parts being uploaded in parallel.
        overrides.setProperty("jclouds.mpu.parallel.degree", "1");
        overrides.setProperty("jclouds.mpu.parts.size", Integer.toString(maxBlockSize));
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        ProviderRegistry.registerProvider(new AWSS3ProviderMetadata());
        ProviderRegistry.registerProvider(new GoogleCloudStorageProviderMetadata());

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(driver);
        contextBuilder.credentials(credentials.identity, credentials.credential);

        if (isS3Driver(driver) && !Strings.isNullOrEmpty(endpoint)) {
            contextBuilder.endpoint(endpoint);
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }
        contextBuilder.overrides(overrides);
        BlobStoreContext context = contextBuilder.buildView(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();

        log.info("Connect to blobstore : driver: {}, region: {}, endpoint: {}",
            driver, region, endpoint);
        return Pair.of(
            BlobStoreLocation.of(region, endpoint),
            blobStore);
    }

    private final VersionCheck VERSION_CHECK = (key, blob) -> {
        // NOTE all metadata in jclouds comes out as lowercase, in an effort to normalize the providers
        String version = blob.getMetadata().getUserMetadata().get(METADATA_FORMAT_VERSION_KEY.toLowerCase());
        if (version == null || !version.equals(CURRENT_VERSION)) {
            throw new IOException(String.format("Invalid object version %s for %s, expect %s",
                version, key, CURRENT_VERSION));
        }
    };

    private final OrderedScheduler scheduler;

    // container in jclouds to write offloaded ledgers
    private final String writeBucket;
    // the region to write offloaded ledgers
    private final String writeRegion;
    // the endpoint
    private final String writeEndpoint;
    // credentials
    private final Credentials credentials;

    // max block size for each data block.
    private int maxBlockSize;
    private final int readBufferSize;

    private final BlobStore writeBlobStore;
    private final Location writeLocation;

    private final ConcurrentMap<BlobStoreLocation, BlobStore> readBlobStores = new ConcurrentHashMap<>();

    // metadata to be stored as part of the offloaded ledger metadata
    private final Map<String, String> userMetadata;
    // offload driver metadata to be stored as part of the original ledger metadata
    private final String offloadDriverName;

    @VisibleForTesting
    static BlobStoreManagedLedgerOffloader create(TieredStorageConfigurationData conf,
                                                  OrderedScheduler scheduler) throws IOException {
        return create(conf, Maps.newHashMap(), scheduler);
    }

    public static BlobStoreManagedLedgerOffloader create(TieredStorageConfigurationData conf,
                                                         Map<String, String> userMetadata,
                                                         OrderedScheduler scheduler)
            throws IOException {
        String driver = conf.getManagedLedgerOffloadDriver();
        if ("s3".equals(driver.toLowerCase())) {
            driver = "aws-s3";
        }
        if (!driverSupported(driver)) {
            throw new IOException(
                "Not support this kind of driver as offload backend: " + driver);
        }

        String endpoint = conf.getS3ManagedLedgerOffloadServiceEndpoint();
        String region = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadRegion() :
            conf.getGcsManagedLedgerOffloadRegion();
        String bucket = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadBucket() :
            conf.getGcsManagedLedgerOffloadBucket();
        int maxBlockSize = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadMaxBlockSizeInBytes() :
            conf.getGcsManagedLedgerOffloadMaxBlockSizeInBytes();
        int readBufferSize = isS3Driver(driver) ?
            conf.getS3ManagedLedgerOffloadReadBufferSizeInBytes() :
            conf.getGcsManagedLedgerOffloadReadBufferSizeInBytes();

        if (isS3Driver(driver) && Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
            throw new IOException(
                    "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                    + " if s3 offload enabled");
        }

        if (Strings.isNullOrEmpty(bucket)) {
            throw new IOException(
                "ManagedLedgerOffloadBucket cannot be empty for s3 and gcs offload");
        }
        if (maxBlockSize < 5*1024*1024) {
            throw new IOException(
                "ManagedLedgerOffloadMaxBlockSizeInBytes cannot be less than 5MB for s3 and gcs offload");
        }

        Credentials credentials = getCredentials(driver, conf);

        return new BlobStoreManagedLedgerOffloader(driver, bucket, scheduler,
            maxBlockSize, readBufferSize, endpoint, region, credentials, userMetadata);
    }

    public static Credentials getCredentials(String driver, TieredStorageConfigurationData conf) throws IOException {
        // credentials:
        //   for s3, get by DefaultAWSCredentialsProviderChain.
        //   for gcs, use downloaded file 'google_creds.json', which contains service account key by
        //     following instructions in page https://support.google.com/googleapi/answer/6158849

        if (isGcsDriver(driver)) {
            String gcsKeyPath = conf.getGcsManagedLedgerOffloadServiceAccountKeyFile();
            if (Strings.isNullOrEmpty(gcsKeyPath)) {
                throw new IOException(
                    "The service account key path is empty for GCS driver");
            }
            try {
                String gcsKeyContent = Files.toString(new File(gcsKeyPath), Charset.defaultCharset());
                return new GoogleCredentialsFromJson(gcsKeyContent).get();
            } catch (IOException ioe) {
                log.error("Cannot read GCS service account credentials file: {}", gcsKeyPath);
                throw new IOException(ioe);
            }
        } else if (isS3Driver(driver)) {
            AWSCredentials credentials = null;
            try {
                DefaultAWSCredentialsProviderChain creds = DefaultAWSCredentialsProviderChain.getInstance();
                credentials = creds.getCredentials();
            } catch (Exception e) {
                // allowed, some mock s3 service not need credential
                log.warn("Exception when get credentials for s3 ", e);
            }

            String id = "accesskey";
            String key = "secretkey";
            if (credentials != null) {
                id = credentials.getAWSAccessKeyId();
                key = credentials.getAWSSecretKey();
            }
            return new Credentials(id, key);
        } else {
            throw new IOException(
                "Not support this kind of driver: " + driver);
        }
    }


    // build context for jclouds BlobStoreContext
    BlobStoreManagedLedgerOffloader(String driver, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize, String endpoint, String region, Credentials credentials) {
        this(driver, container, scheduler, maxBlockSize, readBufferSize, endpoint, region, credentials, Maps.newHashMap());
    }

    BlobStoreManagedLedgerOffloader(String driver, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize,
                                    String endpoint, String region, Credentials credentials,
                                    Map<String, String> userMetadata) {
        this.offloadDriverName = driver;
        this.scheduler = scheduler;
        this.readBufferSize = readBufferSize;
        this.writeBucket = container;
        this.writeRegion = region;
        this.writeEndpoint = endpoint;
        this.maxBlockSize = maxBlockSize;
        this.userMetadata = userMetadata;
        this.credentials = credentials;

        if (!Strings.isNullOrEmpty(region)) {
            this.writeLocation = new LocationBuilder()
                .scope(LocationScope.REGION)
                .id(region)
                .description(region)
=======
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;

/**
 * Tiered Storage Offloader that is backed by a JCloud Blob Store.
 * <p>
 * The constructor takes an instance of TieredStorageConfiguration, which
 * contains all of the configuration data necessary to connect to a JCloud
 * Provider service.
 * </p>
 */
@Slf4j
public class BlobStoreManagedLedgerOffloader implements LedgerOffloader {

    private final OrderedScheduler scheduler;
    private final TieredStorageConfiguration config;
//    private final BlobStore writeBlobStore;
    private final Location writeLocation;

    // metadata to be stored as part of the offloaded ledger metadata
    private final Map<String, String> userMetadata;

    private final ConcurrentMap<BlobStoreLocation, BlobStore> blobStores = new ConcurrentHashMap<>();

    public static BlobStoreManagedLedgerOffloader create(TieredStorageConfiguration config,
            Map<String, String> userMetadata,
            OrderedScheduler scheduler) throws IOException {

        return new BlobStoreManagedLedgerOffloader(config, scheduler, userMetadata);
    }

    BlobStoreManagedLedgerOffloader(TieredStorageConfiguration config, OrderedScheduler scheduler,
                                    Map<String, String> userMetadata) {

        this.scheduler = scheduler;
        this.userMetadata = userMetadata;
        this.config = config;

        if (!Strings.isNullOrEmpty(config.getRegion())) {
            this.writeLocation = new LocationBuilder()
                .scope(LocationScope.REGION)
                .id(config.getRegion())
                .description(config.getRegion())
>>>>>>> f773c602c... Test pr 10 (#27)
                .build();
        } else {
            this.writeLocation = null;
        }

        log.info("Constructor offload driver: {}, host: {}, container: {}, region: {} ",
<<<<<<< HEAD
            driver, endpoint, container, region);

        Pair<BlobStoreLocation, BlobStore> blobStore = createBlobStore(
            driver, region, endpoint, credentials, maxBlockSize
        );
        this.writeBlobStore = blobStore.getRight();
        this.readBlobStores.put(blobStore.getLeft(), blobStore.getRight());
    }

    // build context for jclouds BlobStoreContext, mostly used in test
    @VisibleForTesting
    BlobStoreManagedLedgerOffloader(BlobStore blobStore, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize) {
        this(blobStore, container, scheduler, maxBlockSize, readBufferSize, Maps.newHashMap());
    }

    BlobStoreManagedLedgerOffloader(BlobStore blobStore, String container, OrderedScheduler scheduler,
                                    int maxBlockSize, int readBufferSize,
                                    Map<String, String> userMetadata) {
        this.offloadDriverName = "aws-s3";
        this.scheduler = scheduler;
        this.readBufferSize = readBufferSize;
        this.writeBucket = container;
        this.writeRegion = null;
        this.writeEndpoint = null;
        this.maxBlockSize = maxBlockSize;
        this.writeBlobStore = blobStore;
        this.writeLocation = null;
        this.userMetadata = userMetadata;
        this.credentials = null;

        readBlobStores.put(
            BlobStoreLocation.of(writeRegion, writeEndpoint),
            blobStore
        );
    }

    static String dataBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d", uuid.toString(), ledgerId);
    }

    static String indexBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d-index", uuid.toString(), ledgerId);
    }

    public boolean createBucket(String bucket) {
        return writeBlobStore.createContainerInLocation(writeLocation, bucket);
    }

    public void deleteBucket(String bucket) {
        writeBlobStore.deleteContainer(bucket);
=======
                config.getProvider().getDriver(), config.getServiceEndpoint(),
                config.getBucket(), config.getRegion());

        blobStores.putIfAbsent(config.getBlobStoreLocation(), config.getBlobStore());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public String getOffloadDriverName() {
<<<<<<< HEAD
        return offloadDriverName;
=======
        return config.getDriver();
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
<<<<<<< HEAD
        return ImmutableMap.of(
            METADATA_FIELD_BUCKET, writeBucket,
            METADATA_FIELD_REGION, writeRegion,
            METADATA_FIELD_ENDPOINT, writeEndpoint
        );
    }

    // upload DataBlock to s3 using MultiPartUpload, and indexBlock in a new Block,
=======
        return config.getOffloadDriverMetadata();
    }

//    @VisibleForTesting
//    public ConcurrentMap<BlobStoreLocation, BlobStore> getBlobStores() {
//        return blobStores;
//    }

    /**
     * Upload the DataBlocks associated with the given ReadHandle using MultiPartUpload,
     * Creating indexBlocks for each corresponding DataBlock that is uploaded.
     */
>>>>>>> f773c602c... Test pr 10 (#27)
    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle,
                                           UUID uuid,
                                           Map<String, String> extraMetadata) {
<<<<<<< HEAD
=======
        final BlobStore writeBlobStore = blobStores.get(config.getBlobStoreLocation());
>>>>>>> f773c602c... Test pr 10 (#27)
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).submit(() -> {
            if (readHandle.getLength() == 0 || !readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
                promise.completeExceptionally(
                        new IllegalArgumentException("An empty or open ledger should never be offloaded"));
                return;
            }
            OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create()
                .withLedgerMetadata(readHandle.getLedgerMetadata())
                .withDataBlockHeaderLength(BlockAwareSegmentInputStreamImpl.getHeaderSize());
<<<<<<< HEAD
            String dataBlockKey = dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexBlockKey = indexBlockOffloadKey(readHandle.getId(), uuid);
=======
            String dataBlockKey = DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexBlockKey = DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid);
>>>>>>> f773c602c... Test pr 10 (#27)

            MultipartUpload mpu = null;
            List<MultipartPart> parts = Lists.newArrayList();

            // init multi part upload for data block.
            try {
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(dataBlockKey);
<<<<<<< HEAD
                addVersionInfo(blobBuilder, userMetadata);
                Blob blob = blobBuilder.build();
                mpu = writeBlobStore.initiateMultipartUpload(writeBucket, blob.getMetadata(), new PutOptions());
=======
                DataBlockUtils.addVersionInfo(blobBuilder, userMetadata);
                Blob blob = blobBuilder.build();
                mpu = writeBlobStore.initiateMultipartUpload(config.getBucket(), blob.getMetadata(), new PutOptions());
>>>>>>> f773c602c... Test pr 10 (#27)
            } catch (Throwable t) {
                promise.completeExceptionally(t);
                return;
            }

            long dataObjectLength = 0;
            // start multi part upload for data block.
            try {
                long startEntry = 0;
                int partId = 1;
                long entryBytesWritten = 0;
                while (startEntry <= readHandle.getLastAddConfirmed()) {
                    int blockSize = BlockAwareSegmentInputStreamImpl
<<<<<<< HEAD
                        .calculateBlockSize(maxBlockSize, readHandle, startEntry, entryBytesWritten);
=======
                        .calculateBlockSize(config.getMaxBlockSizeInBytes(), readHandle, startEntry, entryBytesWritten);
>>>>>>> f773c602c... Test pr 10 (#27)

                    try (BlockAwareSegmentInputStream blockStream = new BlockAwareSegmentInputStreamImpl(
                        readHandle, startEntry, blockSize)) {

                        Payload partPayload = Payloads.newInputStreamPayload(blockStream);
<<<<<<< HEAD
                        partPayload.getContentMetadata().setContentLength((long)blockSize);
                        partPayload.getContentMetadata().setContentType("application/octet-stream");
                        parts.add(writeBlobStore.uploadMultipartPart(mpu, partId, partPayload));
                        log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                            writeBucket, dataBlockKey, partId, mpu.id());
=======
                        partPayload.getContentMetadata().setContentLength((long) blockSize);
                        partPayload.getContentMetadata().setContentType("application/octet-stream");
                        parts.add(writeBlobStore.uploadMultipartPart(mpu, partId, partPayload));
                        log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                                config.getBucket(), dataBlockKey, partId, mpu.id());
>>>>>>> f773c602c... Test pr 10 (#27)

                        indexBuilder.addBlock(startEntry, partId, blockSize);

                        if (blockStream.getEndEntryId() != -1) {
                            startEntry = blockStream.getEndEntryId() + 1;
                        } else {
                            // could not read entry from ledger.
                            break;
                        }
                        entryBytesWritten += blockStream.getBlockEntryBytesCount();
                        partId++;
                    }

                    dataObjectLength += blockSize;
                }

                writeBlobStore.completeMultipartUpload(mpu, parts);
                mpu = null;
            } catch (Throwable t) {
                try {
                    if (mpu != null) {
                        writeBlobStore.abortMultipartUpload(mpu);
                    }
                } catch (Throwable throwable) {
                    log.error("Failed abortMultipartUpload in bucket - {} with key - {}, uploadId - {}.",
<<<<<<< HEAD
                        writeBucket, dataBlockKey, mpu.id(), throwable);
=======
                            config.getBucket(), dataBlockKey, mpu.id(), throwable);
>>>>>>> f773c602c... Test pr 10 (#27)
                }
                promise.completeExceptionally(t);
                return;
            }

            // upload index block
            try (OffloadIndexBlock index = indexBuilder.withDataObjectLength(dataObjectLength).build();
                 OffloadIndexBlock.IndexInputStream indexStream = index.toStream()) {
                // write the index block
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(indexBlockKey);
<<<<<<< HEAD
                addVersionInfo(blobBuilder, userMetadata);
                Payload indexPayload = Payloads.newInputStreamPayload(indexStream);
                indexPayload.getContentMetadata().setContentLength((long)indexStream.getStreamSize());
=======
                DataBlockUtils.addVersionInfo(blobBuilder, userMetadata);
                Payload indexPayload = Payloads.newInputStreamPayload(indexStream);
                indexPayload.getContentMetadata().setContentLength((long) indexStream.getStreamSize());
>>>>>>> f773c602c... Test pr 10 (#27)
                indexPayload.getContentMetadata().setContentType("application/octet-stream");

                Blob blob = blobBuilder
                    .payload(indexPayload)
<<<<<<< HEAD
                    .contentLength((long)indexStream.getStreamSize())
                    .build();

                writeBlobStore.putBlob(writeBucket, blob);
                promise.complete(null);
            } catch (Throwable t) {
                try {
                    writeBlobStore.removeBlob(writeBucket, dataBlockKey);
                } catch (Throwable throwable) {
                    log.error("Failed deleteObject in bucket - {} with key - {}.",
                        writeBucket, dataBlockKey, throwable);
=======
                    .contentLength((long) indexStream.getStreamSize())
                    .build();

                writeBlobStore.putBlob(config.getBucket(), blob);
                promise.complete(null);
            } catch (Throwable t) {
                try {
                    writeBlobStore.removeBlob(config.getBucket(), dataBlockKey);
                } catch (Throwable throwable) {
                    log.error("Failed deleteObject in bucket - {} with key - {}.",
                            config.getBucket(), dataBlockKey, throwable);
>>>>>>> f773c602c... Test pr 10 (#27)
                }
                promise.completeExceptionally(t);
                return;
            }
        });
        return promise;
    }

<<<<<<< HEAD
    String getReadRegion(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(METADATA_FIELD_REGION, writeRegion);
    }

    String getReadBucket(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(METADATA_FIELD_BUCKET, writeBucket);
    }

    String getReadEndpoint(Map<String, String> offloadDriverMetadata) {
        return offloadDriverMetadata.getOrDefault(METADATA_FIELD_ENDPOINT, writeEndpoint);
    }

    BlobStore getReadBlobStore(Map<String, String> offloadDriverMetadata) {
        BlobStoreLocation location = BlobStoreLocation.of(
            getReadRegion(offloadDriverMetadata),
            getReadEndpoint(offloadDriverMetadata)
        );
        BlobStore blobStore = readBlobStores.get(location);
        if (null == blobStore) {
            blobStore = createBlobStore(
                offloadDriverName,
                location.getRegion(),
                location.getEndpoint(),
                credentials,
                maxBlockSize
            ).getRight();
            BlobStore existingBlobStore = readBlobStores.putIfAbsent(location, blobStore);
            if (null == existingBlobStore) {
                return blobStore;
            } else {
                return existingBlobStore;
            }
        } else {
            return blobStore;
        }
=======
    /**
     * Attempts to create a BlobStoreLocation from the values in the offloadDriverMetadata,
     * however, if no values are available, it defaults to the currently configured
     * provider, region, bucket, etc.
     *
     * @param offloadDriverMetadata
     * @return
     */
    private BlobStoreLocation getBlobStoreLocation(Map<String, String> offloadDriverMetadata) {
        return (!offloadDriverMetadata.isEmpty()) ? new BlobStoreLocation(offloadDriverMetadata) :
            new BlobStoreLocation(getOffloadDriverMetadata());
>>>>>>> f773c602c... Test pr 10 (#27)
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {
<<<<<<< HEAD
        String readBucket = getReadBucket(offloadDriverMetadata);
        BlobStore readBlobstore = getReadBlobStore(offloadDriverMetadata);

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String key = dataBlockOffloadKey(ledgerId, uid);
        String indexKey = indexBlockOffloadKey(ledgerId, uid);
=======

        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket();
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String key = DataBlockUtils.dataBlockOffloadKey(ledgerId, uid);
        String indexKey = DataBlockUtils.indexBlockOffloadKey(ledgerId, uid);
>>>>>>> f773c602c... Test pr 10 (#27)
        scheduler.chooseThread(ledgerId).submit(() -> {
                try {
                    promise.complete(BlobStoreBackedReadHandleImpl.open(scheduler.chooseThread(ledgerId),
                                                                 readBlobstore,
                                                                 readBucket, key, indexKey,
<<<<<<< HEAD
                                                                 VERSION_CHECK,
                                                                 ledgerId, readBufferSize));
=======
                                                                 DataBlockUtils.VERSION_CHECK,
                                                                 ledgerId, config.getReadBufferSizeInBytes()));
>>>>>>> f773c602c... Test pr 10 (#27)
                } catch (Throwable t) {
                    log.error("Failed readOffloaded: ", t);
                    promise.completeExceptionally(t);
                }
            });
        return promise;
    }

<<<<<<< HEAD


    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                                                   Map<String, String> offloadDriverMetadata) {
        String readBucket = getReadBucket(offloadDriverMetadata);
        BlobStore readBlobstore = getReadBlobStore(offloadDriverMetadata);
=======
    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                                                   Map<String, String> offloadDriverMetadata) {
        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket(offloadDriverMetadata);
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());
>>>>>>> f773c602c... Test pr 10 (#27)

        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                readBlobstore.removeBlobs(readBucket,
<<<<<<< HEAD
                    ImmutableList.of(dataBlockOffloadKey(ledgerId, uid), indexBlockOffloadKey(ledgerId, uid)));
=======
                    ImmutableList.of(DataBlockUtils.dataBlockOffloadKey(ledgerId, uid),
                                     DataBlockUtils.indexBlockOffloadKey(ledgerId, uid)));
>>>>>>> f773c602c... Test pr 10 (#27)
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete Blob", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }

<<<<<<< HEAD
    public interface VersionCheck {
        void check(String key, Blob blob) throws IOException;
    }
}


=======
    
    @Override
    public OffloadPolicies getOffloadPolicies() {
        // TODO Auto-generated method stub
        Properties properties = new Properties();
        properties.putAll(config.getConfigProperties());
        return OffloadPolicies.create(properties);
    }

    @Override
    public void close() {
        for (BlobStore readBlobStore : blobStores.values()) {
            if (readBlobStore != null) {
                readBlobStore.getContext().close();
            }
        }
    }
}
>>>>>>> f773c602c... Test pr 10 (#27)
