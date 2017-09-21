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

import com.emc.object.s3.S3Config;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.DeleteObjectsRequest;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.impl.IdempotentStorageTestBase;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Unit tests for ExtendedS3Storage.
 */
@Slf4j
public class ExtendedS3StorageTest extends IdempotentStorageTestBase {
    private static final String BUCKET_NAME_PREFIX = "pravegatest-";
    private ExtendedS3StorageFactory storageFactory;
    private ExtendedS3StorageConfig adapterConfig;
    private S3JerseyClient client = null;
    private S3ImplBase s3Proxy;
    private final int port = TestUtils.getAvailableListenPort();
    private final String endpoint = "http://127.0.0.1:" + port;
    private S3Config s3Config;

    @Before
    public void setUp() throws Exception {
        String bucketName = BUCKET_NAME_PREFIX + UUID.randomUUID().toString();
        this.adapterConfig = ExtendedS3StorageConfig.builder()
                                                    .with(ExtendedS3StorageConfig.BUCKET, bucketName)
                                                    .with(ExtendedS3StorageConfig.ACCESS_KEY_ID, "x")
                                                    .with(ExtendedS3StorageConfig.SECRET_KEY, "x")
                                                    .with(ExtendedS3StorageConfig.ROOT, "test")
                                                    .with(ExtendedS3StorageConfig.URI, endpoint)
                                                    .build();
        URI uri = URI.create(endpoint);
        s3Config = new S3Config(uri);
        s3Config = s3Config.withIdentity(adapterConfig.getAccessKey()).withSecretKey(adapterConfig.getSecretKey());
        s3Proxy = new S3ProxyImpl(endpoint, s3Config);
        s3Proxy.start();
        createStorage();
        client.createBucket(bucketName);
        List<ObjectKey> keys = client.listObjects(bucketName).getObjects().stream().map(object -> {
            return new ObjectKey(object.getKey());
        }).collect(Collectors.toList());

        if (!keys.isEmpty()) {
            client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
        }
    }

    @After
    public void tearDown() throws Exception {
        client.shutdown();
        client = null;
        s3Proxy.stop();
    }

    //region If-none-match test
    /**
     * Tests the create() method with if-none-match set. Note that we currently
     * do not run a real storage tier, so we cannot verify the behavior of the
     * option against a real storage. Here instead, we are simply making sure
     * that the new execution path does not break anything.
     */
    @Test
    public void testCreateIfNoneMatch() {
        adapterConfig = ExtendedS3StorageConfig.builder()
                                               .with(ExtendedS3StorageConfig.BUCKET, adapterConfig.getBucket())
                                               .with(ExtendedS3StorageConfig.ACCESS_KEY_ID, "x")
                                               .with(ExtendedS3StorageConfig.SECRET_KEY, "x")
                                               .with(ExtendedS3StorageConfig.ROOT, "test")
                                               .with(ExtendedS3StorageConfig.URI, endpoint)
                                               .with(ExtendedS3StorageConfig.USENONEMATCH, true)
                                               .build();
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            s.create(segmentName, null).join();
            assertThrows("create() did not throw for existing StreamSegment.",
                    s.create(segmentName, null),
                    ex -> ex instanceof StreamSegmentExistsException);
        }
    }
    //endregion

    @Override
    protected Storage createStorage() {

        client = new S3JerseyClientWrapper(s3Config, s3Proxy);

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
}
