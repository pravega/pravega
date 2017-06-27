/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CopyObjectResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.exts3.EXTS3StorageTest;
import io.pravega.segmentstore.storage.impl.exts3.ExtS3Storage;
import io.pravega.segmentstore.storage.impl.exts3.ExtS3StorageConfig;
import io.pravega.segmentstore.storage.impl.exts3.ExtS3StorageFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class ECSIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 3;
    private BookKeeperRunner bookkeeper = null;
    private S3Proxy s3Proxy;

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        bookkeeper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        bookkeeper.initialize();

        String endpoint = "http://127.0.0.1:9020";
        URI uri = URI.create(endpoint);
        Properties properties = new Properties();
        properties.setProperty("s3proxy.authorization", "none");
        properties.setProperty("s3proxy.endpoint", endpoint);
        properties.setProperty("jclouds.provider", "filesystem");
        properties.setProperty("jclouds.filesystem.basedir", "/tmp/s3proxy");
/*
        ContextBuilder builder = ContextBuilder
                .newBuilder("filesystem")
                .credentials("x", "x")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(properties);
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();

        s3Proxy = S3Proxy.builder().awsAuthentication( AuthenticationType.AWS_V2_OR_V4, "x", "x")
                         .endpoint(uri)
                         .keyStore("", "")
                         .blobStore(blobStore)
                         .ignoreUnknownHeaders(true)
                         .build();

        s3Proxy.start();
*/
        this.configBuilder.include(ExtS3StorageConfig.builder()
                         .with(ExtS3StorageConfig.EXTS3_BUCKET, "kanpravegatest")
                         .with(ExtS3StorageConfig.EXTS3_ACCESS_KEY_ID, "x")
                         .with(ExtS3StorageConfig.EXTS3_SECRET_KEY, "x")
                         .with(ExtS3StorageConfig.EXTS3_URI, "http://localhost:9020")
                         .with(ExtS3StorageConfig.ROOT, "test"));

    }

    /**
     * Shuts down BookKeeper and cleans up file system directory
     */
    @After
    public void tearDown() throws Exception {
        bookkeeper.close();
        //s3Proxy.stop();
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    StorageFactory f = new ExtS3StorageFactory(
                            setup.getConfig(ExtS3StorageConfig::builder), setup.getExecutor());
                    return new ExtS3ListenableStorageFactory(f, builderConfig.getConfig(ExtS3StorageConfig::builder));
                })
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        bookkeeper.getZkClient(), setup.getExecutor()));
    }

    private class ExtS3ListenableStorageFactory extends ListenableStorageFactory {

        private final ExtS3StorageConfig adapterConfig;

        public ExtS3ListenableStorageFactory(StorageFactory wrappedFactory, ExtS3StorageConfig adapterConfig) {
            super(wrappedFactory);
            this.adapterConfig = adapterConfig;
        }



        @Override
        public Storage createStorageAdapter() {
            S3Config exts3Config = null;
            try {
                exts3Config = new S3Config(new URI("http://localhost:9020"));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            exts3Config.withIdentity(adapterConfig.getExts3AccessKey()).withSecretKey(adapterConfig.getExts3SecretKey());


            S3JerseyClient client = new S3ClientWrapper(exts3Config);

            Storage storage = this.wrappedFactory.createStorageAdapter();
            this.storage = storage;
            ((ExtS3Storage) storage).setClient(client);
            return storage;
        }

        private class S3ClientWrapper extends S3JerseyClient {
            private String baseDir;

            public S3ClientWrapper(S3Config exts3Config) {
                super(exts3Config);
                try {
                    this.baseDir = Files.createTempDirectory("exts3_wrapper").toString();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            private final ConcurrentMap<String, EXTS3StorageTest.AclSize> ACL_MAP = new ConcurrentHashMap<>();


            @Override
            public PutObjectResult putObject(PutObjectRequest request) {
                S3ObjectMetadata metadata = request.getObjectMetadata();

                if ( request.getObjectMetadata() != null ) {
                    request.setObjectMetadata(null);
                }
                try {
                    Files.createFile(Paths.get(this.baseDir, request.getBucketName(), request.getKey()));
                } catch (IOException e) {
                    throw new S3Exception(e.getMessage(),0);
                }
                PutObjectResult retVal = new PutObjectResult();
                if (request.getAcl() != null) {
                    long size = 0;
                    if (request.getRange() != null) {
                        size = request.getRange().getLast() -1;
                    }
                    ACL_MAP.put(request.getKey(), new EXTS3StorageTest.AclSize(request.getAcl(),
                            size));
                }
                return retVal;
            }

            @Override
            public void putObject(String bucketName, String key, Range range, Object content) {
                byte[] totalByes = new byte[Math.toIntExact(range.getLast() + 1)];
                try {
                    if ( range.getFirst() != 0 ) {
                        int bytesRead = getObject(bucketName, key).getObject().read(totalByes, 0,
                                Math.toIntExact(range.getFirst()));
                        if ( bytesRead != range.getFirst() ) {
                            throw new CompletionException(new BadOffsetException(key, range.getFirst(), bytesRead));
                        }
                    }
                    int bytesRead = ( (InputStream) content).read(totalByes, Math.toIntExact(range.getFirst()),
                            Math.toIntExact(range.getLast() + 1 - range.getFirst()));

                    if ( bytesRead != range.getLast() + 1 - range.getFirst()) {
                        throw new IllegalArgumentException();
                    }
                    Path path = Paths.get(this.baseDir, bucketName, key);
                    FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE);
                   // channel.write()
                    ACL_MAP.get(key).setSize(range.getLast() -1);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
                EXTS3StorageTest.AclSize retVal = ACL_MAP.get(key);
                if ( retVal == null ) {
                    throw new S3Exception("NoObject", 500, "NoSuchKey", key);
                }
                retVal.setAcl(acl);
            }

            @Override
            public void setObjectAcl(SetObjectAclRequest request) {
                EXTS3StorageTest.AclSize retVal = ACL_MAP.get(request.getKey());
                if ( retVal == null ) {
                    throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
                }
                retVal.setAcl(request.getAcl());
            }

            @Override
            public AccessControlList getObjectAcl(String bucketName, String key) {
                EXTS3StorageTest.AclSize retVal = ACL_MAP.get(key);
                if ( retVal == null ) {
                    throw new S3Exception("NoObject", 500, "NoSuchKey", key);
                }
                return retVal.getAcl();
            }

            public CopyPartResult copyPart(CopyPartRequest request) {
                if ( ACL_MAP.get(request.getKey()) == null ) {
                    throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
                }

                Range range = request.getSourceRange();
                if ( range.getLast() == -1 ) {
                    range = Range.fromOffsetLength(0, ACL_MAP.get(request.getSourceKey()).getSize());
                    request.withSourceRange(range);
                }
                CopyObjectResult result = executeRequest(client, request, CopyObjectResult.class);
                CopyPartResult retVal = new CopyPartResult();
                retVal.setPartNumber(request.getPartNumber());
                retVal.setETag(result.getETag());
                return retVal;
            }

            @Override
            public void deleteObject(String bucketName, String key) {
                super.deleteObject(bucketName, key);
                ACL_MAP.remove(key);
            }

            @Override
            public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
                request.getDeleteObjects().getKeys().forEach( (key) -> ACL_MAP.remove(key));
                return super.deleteObjects(request);
            }
        }
    }

    //endregion
}
