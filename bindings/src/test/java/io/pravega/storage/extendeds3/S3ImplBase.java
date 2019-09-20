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
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for S3 server emulation for testing extended S3 Storage implementation.
 */
public abstract class S3ImplBase {
    protected final ConcurrentMap<String, AclSize> aclMap = new ConcurrentHashMap<>();

    public abstract void start() throws Exception;

    public abstract void stop() throws Exception;

    public abstract PutObjectResult putObject(PutObjectRequest request);

    public abstract void putObject(String bucketName, String key, Range range, Object content);

    public abstract void setObjectAcl(String bucketName, String key, AccessControlList acl);

    public abstract void setObjectAcl(SetObjectAclRequest request);

    public abstract AccessControlList getObjectAcl(String bucketName, String key);

    public abstract CopyPartResult copyPart(CopyPartRequest request);

    public abstract void deleteObject(String bucketName, String key);

    public abstract DeleteObjectsResult deleteObjects(DeleteObjectsRequest request);

    public abstract ListObjectsResult listObjects(String bucketName, String prefix);

    public abstract S3ObjectMetadata getObjectMetadata(String bucketName, String key);

    public abstract InputStream readObjectStream(String bucketName, String key, Range range);

    public abstract String initiateMultipartUpload(String bucketName, String key);

    public abstract CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request);

    public abstract GetObjectResult<InputStream> getObject(String bucketName, String key);
}
