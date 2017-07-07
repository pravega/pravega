/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.extendeds3;

import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CopyObjectResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.impl.filesystem.IdempotentStorageTestBase;
import io.pravega.test.common.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Before;

/**
 * Unit tests for ExtendedS3Storage.
 */
@Slf4j
public class ExtendedS3StorageTest extends IdempotentStorageTestBase {
    private static final String BUCKET_NAME = "pravegatest";
    private ExtendedS3StorageFactory storageFactory;
    private ExtendedS3StorageConfig adapterConfig;
    private S3JerseyClient client = null;
    private S3Proxy s3Proxy;
    private final ConcurrentMap<String, AclSize> aclMap = new ConcurrentHashMap<>();
    private final int port = TestUtils.getAvailableListenPort();
    private final String endpoint = "http://127.0.0.1:" + port;

    @Before
    public void setUp() throws Exception {

        URI uri = URI.create(endpoint);
        Properties properties = new Properties();
        properties.setProperty("s3proxy.authorization", "none");
        properties.setProperty("s3proxy.endpoint", endpoint);
        properties.setProperty("jclouds.provider", "filesystem");
        properties.setProperty("jclouds.filesystem.basedir", "/tmp/s3proxy");

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

        this.adapterConfig = ExtendedS3StorageConfig.builder()
                                                    .with(ExtendedS3StorageConfig.BUCKET, BUCKET_NAME)
                                                    .with(ExtendedS3StorageConfig.ACCESS_KEY_ID, "x")
                                                    .with(ExtendedS3StorageConfig.SECRET_KEY, "x")
                                                    .with(ExtendedS3StorageConfig.ROOT, "test")
                                                    .with(ExtendedS3StorageConfig.URI, endpoint)
                                                    .build();
        createStorage();

        try {
            client.createBucket(BUCKET_NAME);
        } catch (S3Exception e) {
            if (!e.getErrorCode().equals("BucketAlreadyOwnedByYou")) {
                throw e;
            }
        }
        List<ObjectKey> keys = client.listObjects(BUCKET_NAME).getObjects().stream().map((object) -> {
            return new ObjectKey(object.getKey());
        }).collect(Collectors.toList());

        if (!keys.isEmpty()) {
            client.deleteObjects(new DeleteObjectsRequest(BUCKET_NAME).withKeys(keys));
        }
    }

    @After
    public void tearDown() throws Exception {
        client.shutdown();
        client = null;
        s3Proxy.stop();
    }

    @Override
    protected Storage createStorage() {
        URI uri = URI.create(endpoint);
        S3Config s3Config = new S3Config(uri);
        s3Config = s3Config.withIdentity(adapterConfig.getAccessKey()).withSecretKey(adapterConfig.getSecretKey());

        client = new S3JerseyClientWrapper(s3Config, aclMap);

        ExtendedS3Storage storage = new ExtendedS3Storage(client, adapterConfig, executorService());
        return storage;
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            return ExtendedS3SegmentHandle.getReadHandle(segmentName);
        } else {
            return ExtendedS3SegmentHandle.getWriteHandle(segmentName);
        }
    }

    /**
     * Wrapper over S3JerseyClient. This implements ACLs, multipart copy and multiple writes to the same object on top of S3Proxy implementation.
     */
    private static class S3JerseyClientWrapper extends S3JerseyClient {
        private final ConcurrentMap<String, AclSize> aclMap;

        public S3JerseyClientWrapper(S3Config s3Config, ConcurrentMap<String, AclSize> aclMap) {
            super(s3Config);
            this.aclMap = aclMap;
        }

        @Synchronized
        @Override
        public PutObjectResult putObject(PutObjectRequest request) {
            S3ObjectMetadata metadata = request.getObjectMetadata();

           if ( request.getObjectMetadata() != null ) {
            request.setObjectMetadata(null);
           }
            PutObjectResult retVal = super.putObject(request);
           if (request.getAcl() != null) {
               long size = 0;
               if (request.getRange() != null) {
                   size = request.getRange().getLast() -1;
               }
               aclMap.put(request.getKey(), new AclSize(request.getAcl(),
                       size));
           }
           return retVal;
        }

        @Synchronized
        @Override
        public void putObject(String bucketName, String key, Range range, Object content) {
            byte[] totalByes = new byte[Math.toIntExact(range.getLast() + 1)];
            try {
                if ( range.getFirst() != 0 ) {
                    int bytesRead = getObject(bucketName, key).getObject().read(totalByes, 0,
                            Math.toIntExact(range.getFirst()));
                    if ( bytesRead != range.getFirst() ) {
                        throw new IllegalStateException("Unable to read from the object " + key);
                    }
                }
                int bytesRead = ( (InputStream) content).read(totalByes, Math.toIntExact(range.getFirst()),
                        Math.toIntExact(range.getLast() + 1 - range.getFirst()));

                if ( bytesRead != range.getLast() + 1 - range.getFirst()) {
                    throw new IllegalStateException("Not able to read from input stream.");
                }
                super.putObject( new PutObjectRequest(bucketName, key, (Object) new ByteArrayInputStream(totalByes)));
                aclMap.put( key, aclMap.get(key).withSize(range.getLast() -1));
            } catch (IOException e) {
                throw new S3Exception("NoObject", 404, "NoSuchKey", key);
            }
        }

        @Synchronized
        @Override
        public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
            AclSize retVal = aclMap.get(key);
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 404, "NoSuchKey", key);
            }
            aclMap.put(key, retVal.withAcl(acl));
        }

        @Synchronized
        @Override
        public void setObjectAcl(SetObjectAclRequest request) {
            AclSize retVal = aclMap.get(request.getKey());
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 404, "NoSuchKey", request.getKey());
            }
            aclMap.put(request.getKey(), retVal.withAcl(request.getAcl()));
        }

        @Override
        public AccessControlList getObjectAcl(String bucketName, String key) {
            AclSize retVal = aclMap.get(key);
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 404, "NoSuchKey", key);
            }
            return retVal.getAcl();
        }

        public CopyPartResult copyPart(CopyPartRequest request) {
            if ( aclMap.get(request.getKey()) == null ) {
                throw new S3Exception("NoObject", 404, "NoSuchKey", request.getKey());
            }

            Range range = request.getSourceRange();
            if ( range.getLast() == -1 ) {
                range = Range.fromOffsetLength(0, aclMap.get(request.getSourceKey()).getSize());
                request.withSourceRange(range);
            }
            CopyObjectResult result = executeRequest(client, request, CopyObjectResult.class);
            CopyPartResult retVal = new CopyPartResult();
            retVal.setPartNumber(request.getPartNumber());
            retVal.setETag(result.getETag());
            return retVal;
        }

        @Synchronized
        @Override
        public void deleteObject(String bucketName, String key) {
            super.deleteObject(bucketName, key);
            aclMap.remove(key);
        }

        @Synchronized
        @Override
        public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
            request.getDeleteObjects().getKeys().forEach( (key) -> aclMap.remove(key));
            return super.deleteObjects(request);
        }
    }
}
