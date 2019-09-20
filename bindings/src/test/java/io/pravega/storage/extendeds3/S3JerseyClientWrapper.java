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
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import lombok.Synchronized;

/**
 * Wrapper over S3JerseyClient. This implements ACLs, multipart copy and multiple writes to the same object on top of S3Proxy implementation.
 */
public class S3JerseyClientWrapper extends S3JerseyClient {
    private final S3ImplBase proxy;

    public S3JerseyClientWrapper(S3Config s3Config, S3ImplBase proxy) {
        super(s3Config);
        this.proxy = proxy;
    }


    @Synchronized
    @Override
    public PutObjectResult putObject(PutObjectRequest request) {
        return proxy.putObject(request);
    }

    @Synchronized
    @Override
    public void putObject(String bucketName, String key, Range range, Object content) {
        proxy.putObject(bucketName, key, range, content);
    }

    @Synchronized
    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
        proxy.setObjectAcl(bucketName, key, acl);
    }

    @Synchronized
    @Override
    public void setObjectAcl(SetObjectAclRequest request) {
        proxy.setObjectAcl(request);
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) {
        return proxy.getObjectAcl(bucketName, key);
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest request) {
        return proxy.copyPart(request);
    }

    @Synchronized
    @Override
    public void deleteObject(String bucketName, String key) {
        proxy.deleteObject(bucketName, key);
    }

    @Synchronized
    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
        return proxy.deleteObjects(request);
    }
}
