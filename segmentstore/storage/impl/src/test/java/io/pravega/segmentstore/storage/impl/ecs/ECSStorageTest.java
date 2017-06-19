/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.ecs;

import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CopyObjectResult;
import com.emc.object.s3.bean.CopyPartResult;
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
import io.pravega.segmentstore.storage.impl.filesystem.IdempotentStorageTest;
import lombok.extern.slf4j.Slf4j;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Before;

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

/**
 * Unit tests for ECSStorage.
 */
@Slf4j
public class ECSStorageTest extends IdempotentStorageTest {
    private ECSStorageConfig adapterConfig;
    private S3JerseyClient client = null;
    private S3Proxy s3Proxy;

    @Before
    public void setUp() throws Exception {
        S3Config ecsConfig = null;

        String endpoint = "http://127.0.0.1:9020";
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

        this.adapterConfig = ECSStorageConfig.builder()
                .with(ECSStorageConfig.ECS_BUCKET, "kanpravegatest")
                .with(ECSStorageConfig.ECS_ACCESS_KEY_ID, "x")
                .with(ECSStorageConfig.ECS_SECRET_KEY, "x")
                .with(ECSStorageConfig.ROOT, "test")
                .build();
        if (client == null) {
            try {
                createStorage();

                try {
                    client.createBucket("kanpravegatest");
                } catch (Exception e) {
                    if (!( e instanceof S3Exception && ((S3Exception) e).getErrorCode().
                            equals("BucketAlreadyOwnedByYou"))) {
                        throw e;
                    }
                }
                List<ObjectKey> keys = client.listObjects("kanpravegatest").getObjects().stream().map((object) -> {
                    return new ObjectKey(object.getKey());
                }).collect(Collectors.toList());

                client.deleteObjects(new DeleteObjectsRequest("kanpravegatest").withKeys(keys));
            } catch (Exception e) {
                log.error("Wrong ECS URI {}. Can not continue.");
            }
        }
    }

    @After
    public void tearDown() {
        try {
            client.shutdown();
            client = null;
            s3Proxy.stop();
        } catch (Exception e) {
        }
    }

    @Override
    protected Storage createStorage() {
        S3Config ecsConfig = null;
        try {
            ecsConfig = new S3Config(new URI("http://localhost:9020"));
            if (adapterConfig == null) {
                setUp();
            }
            ecsConfig.withIdentity(adapterConfig.getEcsAccessKey()).withSecretKey(adapterConfig.getEcsSecretKey());

            client = new S3JerseyClientWrapper(ecsConfig);
            return new ECSStorage(client, this.adapterConfig, executorService());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            return ECSSegmentHandle.getReadHandle(segmentName);
        } else {
            return ECSSegmentHandle.getWriteHandle(segmentName);
        }
    }


    private static class S3JerseyClientWrapper extends S3JerseyClient {
        private static final ConcurrentMap<String, AccessControlList> ACL_MAP = new ConcurrentHashMap<>();

        public S3JerseyClientWrapper(S3Config ecsConfig) {
            super(ecsConfig);
        }

        @Override
        public PutObjectResult putObject(PutObjectRequest request) {
            S3ObjectMetadata metadata = request.getObjectMetadata();

           if ( request.getObjectMetadata() != null ) {
            request.setObjectMetadata(null);
           }
            PutObjectResult retVal = super.putObject(request);
           if (request.getAcl() != null) {
               ACL_MAP.put(request.getKey(), request.getAcl());
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
                        throw new IllegalArgumentException();
                    }
                }
                int bytesRead = ( (InputStream) content).read(totalByes, Math.toIntExact(range.getFirst()),
                        Math.toIntExact(range.getLast() + 1 - range.getFirst()));

                if ( bytesRead != range.getLast() + 1 - range.getFirst()) {
                    throw new IllegalArgumentException();
                }
                super.putObject( new PutObjectRequest(bucketName, key, (Object) new ByteArrayInputStream(totalByes)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
            AccessControlList retVal = ACL_MAP.get(key);
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", key);
            }
            ACL_MAP.put(key, acl);
        }

        @Override
        public void setObjectAcl(SetObjectAclRequest request) {
            AccessControlList retVal = ACL_MAP.get(request.getKey());
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
            }
            ACL_MAP.put(request.getKey(), request.getAcl());
        }

        @Override
        public AccessControlList getObjectAcl(String bucketName, String key) {
            AccessControlList retVal = ACL_MAP.get(key);
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", key);
            }
            return retVal;
        }

        public CopyPartResult copyPart(CopyPartRequest request) {
            if ( ACL_MAP.get(request.getKey()) == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
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
    }
}
