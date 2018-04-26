/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.CopyObjectResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import lombok.Synchronized;
import org.apache.commons.httpclient.HttpStatus;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

/**
 * Extended S3 server emulation implementation based on S3Proxy. This implementation adds ACL management to S3Proxy.
 * S3Proxy service runs in proc and the client talks to it using REST.
 */
public class S3ProxyImpl extends S3ImplBase {
    private final S3Proxy s3Proxy;
    private final S3JerseyClient client;

    public S3ProxyImpl(String endpoint, S3Config s3Config) {
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
        s3Proxy = S3Proxy.builder().awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "x", "x")
                         .endpoint(uri)
                         .keyStore("", "")
                         .blobStore(blobStore)
                         .ignoreUnknownHeaders(true)
                         .build();
        client = new S3JerseyCopyPartClient(s3Config);
    }

    @Override
    public void start() throws Exception {
        s3Proxy.start();
    }

    @Override
    public void stop() throws Exception {
        s3Proxy.stop();
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest request) {
        S3ObjectMetadata metadata = request.getObjectMetadata();

        if (request.getObjectMetadata() != null) {
            request.setObjectMetadata(null);
        }
        PutObjectResult retVal = client.putObject(request);
        if (request.getAcl() != null) {
            long size = 0;
            if (request.getRange() != null) {
                size = request.getRange().getLast() - 1;
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
            if (range.getFirst() != 0) {
                int bytesRead = client.getObject(bucketName, key).getObject().read(totalByes, 0,
                        Math.toIntExact(range.getFirst()));
                if (bytesRead != range.getFirst()) {
                    throw new IllegalStateException("Unable to read from the object " + key);
                }
            }
            int bytesRead = ((InputStream) content).read(totalByes, Math.toIntExact(range.getFirst()),
                    Math.toIntExact(range.getLast() + 1 - range.getFirst()));

            if (bytesRead != range.getLast() + 1 - range.getFirst()) {
                throw new IllegalStateException("Not able to read from input stream.");
            }
            client.putObject(new PutObjectRequest(bucketName, key, (Object) new ByteArrayInputStream(totalByes)));
            aclMap.put(key, aclMap.get(key).withSize(range.getLast() - 1));
        } catch (IOException e) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", key);
        }
    }

    @Synchronized
    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
        AclSize retVal = aclMap.get(key);
        if (retVal == null) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", key);
        }
        aclMap.put(key, retVal.withAcl(acl));
    }

    @Synchronized
    @Override
    public void setObjectAcl(SetObjectAclRequest request) {
        AclSize retVal = aclMap.get(request.getKey());
        if (retVal == null) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", request.getKey());
        }
        aclMap.put(request.getKey(), retVal.withAcl(request.getAcl()));
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) {
        AclSize retVal = aclMap.get(key);
        if (retVal == null) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", key);
        }
        return retVal.getAcl();
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest request) {
        if (aclMap.get(request.getKey()) == null) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", request.getKey());
        }
        return client.copyPart(request);
    }

    @Override
    public void deleteObject(String bucketName, String key) {
        client.deleteObject(bucketName, key);
        aclMap.remove(key);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
        request.getDeleteObjects().getKeys().forEach(key -> aclMap.remove(key));
        return client.deleteObjects(request);
    }

    @Override
    public ListObjectsResult listObjects(String bucketName, String prefix) {
        return client.listObjects(bucketName, prefix);
    }

    @Override
    public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
        return client.getObjectMetadata(bucketName, key);
    }

    @Override
    public InputStream readObjectStream(String bucketName, String key, Range range) {
        return client.readObjectStream(bucketName, key, range);
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String key) {
        return client.initiateMultipartUpload(bucketName, key);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        return client.completeMultipartUpload(request);
    }

    @Override
    public GetObjectResult<InputStream> getObject(String bucketName, String key) {
        return client.getObject(bucketName, key);
    }

    /**
     * Client for interaction with S3Proxy. It implements the copyPart request as a copyObject request.
     */
    private class S3JerseyCopyPartClient extends S3JerseyClient {
        public S3JerseyCopyPartClient(S3Config s3Config) {
            super(s3Config);
        }

        @Override
        public CopyPartResult copyPart(CopyPartRequest request) {
            Range range = request.getSourceRange();
            if (range.getLast() == -1) {
                range = Range.fromOffsetLength(0, aclMap.get(request.getSourceKey()).getSize());
                request.withSourceRange(range);
            }
            CopyObjectResult result = executeRequest(client, request, CopyObjectResult.class);
            CopyPartResult retVal = new CopyPartResult();
            retVal.setPartNumber(request.getPartNumber());
            retVal.setETag(result.getETag());
            return retVal;
        }
    }
}
