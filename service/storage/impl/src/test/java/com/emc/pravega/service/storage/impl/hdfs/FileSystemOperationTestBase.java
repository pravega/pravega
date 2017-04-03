/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Base class for all tests for derived classes from FileSystemOperation.
 */
abstract class FileSystemOperationTestBase {
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

        void setConcatNext(FileDescriptor file, FileDescriptor nextFile) throws IOException {
            this.operation.setConcatNext(file, nextFile);
        }

        Path createEmptyFile(String segmentName, long offset) throws IOException {
            Path result = this.operation.getFilePath(segmentName, offset, this.epoch);
            this.fileSystem
                    .create(result,
                            new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                            false,
                            0,
                            this.config.getReplication(),
                            this.config.getBlockSize(),
                            null)
                    .close();
            return result;
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
