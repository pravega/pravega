/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.s3.S3Config;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.util.ConfigUri;
import io.pravega.test.common.TestUtils;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Test context Extended S3 tests.
 */
public class ExtendedS3TestContext {
    public static final String BUCKET_NAME_PREFIX = "pravegatest-";
    public final ExtendedS3StorageConfig adapterConfig;
    public final S3JerseyClient client;
    public final S3ImplBase s3Proxy;
    public final int port = TestUtils.getAvailableListenPort();
    public final String configUri = "http://127.0.0.1:" + port + "?identity=x&secretKey=x";
    public final S3Config s3Config;

    public ExtendedS3TestContext() throws Exception {
        try {
            String bucketName = BUCKET_NAME_PREFIX + UUID.randomUUID().toString();
            this.adapterConfig = ExtendedS3StorageConfig.builder()
                    .with(ExtendedS3StorageConfig.CONFIGURI, configUri)
                    .with(ExtendedS3StorageConfig.BUCKET, bucketName)
                    .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                    .build();
            s3Config = new ConfigUri<>(S3Config.class).parseUri(configUri);
            s3Proxy = new S3ProxyImpl(configUri, s3Config);
            s3Proxy.start();
            client = new S3JerseyClientWrapper(s3Config, s3Proxy);
            client.createBucket(bucketName);
            List<ObjectKey> keys = client.listObjects(bucketName).getObjects().stream()
                    .map(object -> new ObjectKey(object.getKey()))
                    .collect(Collectors.toList());

            if (!keys.isEmpty()) {
                client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public ExtendedS3TestContext(S3JerseyClient client) throws Exception {
        String bucketName = BUCKET_NAME_PREFIX + UUID.randomUUID().toString();
        this.adapterConfig = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, configUri)
                .with(ExtendedS3StorageConfig.BUCKET, bucketName)
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .build();
        s3Config = new ConfigUri<>(S3Config.class).parseUri(configUri);
        s3Proxy = null;
        this.client = client;
    }

    public void close() throws Exception {
        if (client != null) {
            client.destroy();
        }
        if (s3Proxy != null) {
            s3Proxy.stop();
        }
    }
}