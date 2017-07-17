/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.impl.IdempotentStorageTestBase;
import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Before;

/**
 * Unit tests for FileSystemStorage.
 */
public class FileSystemStorageTest extends IdempotentStorageTestBase {
    private File baseDir = null;
    private FileSystemStorageConfig adapterConfig;
    private FileSystemStorageFactory storageFactory;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        this.adapterConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
        metrics = new FileSystemStorageMetrics();
        this.storageFactory = new FileSystemStorageFactory(adapterConfig, this.executorService(), metrics);
    }

    @After
    public void tearDown() {
        FileHelpers.deleteFileOrDirectory(baseDir);
        baseDir = null;
    }

    @Override
    protected Storage createStorage() {
        return this.storageFactory.createStorageAdapter();
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            return FileSystemSegmentHandle.readHandle(segmentName);
        } else {
            return FileSystemSegmentHandle.writeHandle(segmentName);
        }
    }


}
