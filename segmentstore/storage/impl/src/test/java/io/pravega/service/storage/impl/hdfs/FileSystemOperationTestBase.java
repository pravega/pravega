/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage.impl.hdfs;

import java.io.IOException;
import java.util.List;
import lombok.val;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Base class for all tests for derived classes from FileSystemOperation.
 */
abstract class FileSystemOperationTestBase {
    protected static final int TEST_TIMEOUT_MILLIS = 30 * 1000;

    static TestContext newContext(long epoch, MockFileSystem fileSystem) {
        return new TestContext(epoch, fileSystem);
    }

    static class TestContext extends FileSystemOperation.OperationContext {
        private final DummyOperation operation;

        private TestContext(long epoch, FileSystem fileSystem) {
            super(epoch, fileSystem, HDFSStorageConfig.builder().build());
            this.operation = new DummyOperation("", this);
        }

        Path getFileName(String segmentName, long startOffset) {
            return this.operation.getFilePath(segmentName, startOffset, this.epoch);
        }

        boolean isSealed(FileDescriptor file) throws IOException {
            return this.operation.isSealed(file);
        }

        boolean isReadOnly(FileStatus fs) {
            return this.operation.isReadOnly(fs);
        }

        boolean makeReadOnly(FileDescriptor file) throws IOException {
            return this.operation.makeReadOnly(file);
        }

        Path createEmptyFile(String segmentName, long offset) throws IOException {
            val path = this.operation.getFilePath(segmentName, offset, this.epoch);
            this.fileSystem
                    .create(path,
                            new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                            false,
                            0,
                            this.config.getReplication(),
                            this.config.getBlockSize(),
                            null)
                    .close();
            return path;
        }

        List<FileDescriptor> findAllFiles(String segmentName) throws IOException {
            return this.operation.findAll(segmentName, false);
        }
    }

    private static class DummyOperation extends FileSystemOperation<String> {
        DummyOperation(String target, OperationContext context) {
            super(target, context);
        }
    }
}
