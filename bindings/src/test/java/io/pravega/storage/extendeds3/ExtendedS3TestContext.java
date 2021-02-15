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
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.util.UUID;

/**
 * Test context Extended S3 tests.
 */
public class ExtendedS3TestContext {
    public static final String BUCKET_NAME_PREFIX = "pravegatest-";
    public final ExtendedS3StorageConfig adapterConfig;
    public final S3ClientMock client;
    public final S3Mock s3Mock;
    public final int port;
    public final String configUri;
    public final S3Config s3Config;

    public ExtendedS3TestContext() {
        try {
            this.port = TestUtils.getAvailableListenPort();
            this.configUri = "http://127.0.0.1:" + port + "?identity=x&secretKey=x";
            String bucketName = BUCKET_NAME_PREFIX + UUID.randomUUID().toString();
            this.adapterConfig = ExtendedS3StorageConfig.builder()
                    .with(ExtendedS3StorageConfig.CONFIGURI, configUri)
                    .with(ExtendedS3StorageConfig.BUCKET, bucketName)
                    .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                    .build();
            s3Config = new S3Config(URI.create(configUri))
                    .withRetryEnabled(false)
                    .withInitialRetryDelay(1)
                    .withProperty("com.sun.jersey.client.property.connectTimeout", 100);
            s3Mock = new S3Mock();
            client = new S3ClientMock(s3Config, s3Mock);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public void close() {
        if (client != null) {
            client.destroy();
        }
    }
}