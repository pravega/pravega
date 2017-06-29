/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.exts3;

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
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.impl.filesystem.IdempotentStorageTest;
import lombok.AllArgsConstructor;
import lombok.Data;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Unit tests for ExtS3Storage.
 */
@Slf4j
public class EXTS3StorageTest extends IdempotentStorageTest {
    private ExtS3StorageFactory storageFactory;
    private ExtS3StorageConfig adapterConfig;
    private S3JerseyClient client = null;
    private S3Proxy s3Proxy;

    @Before
    public void setUp() throws Exception {

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

        this.adapterConfig = ExtS3StorageConfig.builder()
                                               .with(ExtS3StorageConfig.EXTS3_BUCKET, "kanpravegatest")
                                               .with(ExtS3StorageConfig.EXTS3_ACCESS_KEY_ID, "x")
                                               .with(ExtS3StorageConfig.EXTS3_SECRET_KEY, "x")
                                               .with(ExtS3StorageConfig.ROOT, "test")
                                               .with(ExtS3StorageConfig.EXTS3_URI, "http://127.0.0.1:9020")
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
                log.error("Wrong EXTS3 URI {}. Can not continue.");
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
        S3Config exts3Config = null;
        try {
            exts3Config = new S3Config(new URI("http://localhost:9020"));
            if (adapterConfig == null) {
                setUp();
            }
            exts3Config.withIdentity(adapterConfig.getExts3AccessKey()).withSecretKey(adapterConfig.getExts3SecretKey());

            client = new S3JerseyClientWrapper(exts3Config);

            ExtS3Storage storage = new ExtS3Storage(client, adapterConfig, executorService());
            return storage;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            return ExtS3SegmentHandle.getReadHandle(segmentName);
        } else {
            return ExtS3SegmentHandle.getWriteHandle(segmentName);
        }
    }

    /**
     * Wrapper over S3JerseyClient. This implements ACLs, multipart copy and multiple writes to the same object on top of S3Proxy implementation.
     */
    public static class S3JerseyClientWrapper extends S3JerseyClient {
        private static final ConcurrentMap<String, AclSize> ACL_MAP = new ConcurrentHashMap<>();

        public S3JerseyClientWrapper(S3Config exts3Config) {
            super(exts3Config);
        }

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
               ACL_MAP.put(request.getKey(), new AclSize(request.getAcl(),
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
                super.putObject( new PutObjectRequest(bucketName, key, (Object) new ByteArrayInputStream(totalByes)));
                ACL_MAP.get(key).setSize(range.getLast() -1);
            } catch (IOException e) {
            }
        }

        @Override
        public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
            AclSize retVal = ACL_MAP.get(key);
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", key);
            }
            retVal.setAcl(acl);
        }

        @Override
        public void setObjectAcl(SetObjectAclRequest request) {
            AclSize retVal = ACL_MAP.get(request.getKey());
            if ( retVal == null ) {
                throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
            }
            retVal.setAcl(request.getAcl());
        }

        @Override
        public AccessControlList getObjectAcl(String bucketName, String key) {
            AclSize retVal = ACL_MAP.get(key);
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

    @Data
    @AllArgsConstructor
    public static class AclSize {
       private  AccessControlList acl;
       private  long size;
    }
}
