/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageFullException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import io.pravega.segmentstore.storage.chunklayer.InvalidOffsetException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.DiskChecker;

/***
 *  {@link ChunkStorage} for HDFS based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * The concat operation is implemented using HDFS native concat operation.
 */

@Slf4j
public class HDFSChunkStorage extends BaseChunkStorage {
    private static final FsPermission READWRITE_PERMISSION = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    private static final FsPermission READONLY_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);

    //region Members

    private final HDFSStorageConfig config;
    private FileSystem fileSystem;
    private final AtomicBoolean closed;
    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param config The configuration to use.
     */
    HDFSChunkStorage(HDFSStorageConfig config, Executor executor) {
        super(executor);
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        this.closed = new AtomicBoolean(false);
        initialize();
    }

    //endregion

    //region capabilities

    @Override
    public boolean supportsConcat() {
        return true;
    }

    @Override
    public boolean supportsAppend() {
        return true;
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.fileSystem != null) {
                try {
                    this.fileSystem.close();
                    this.fileSystem = null;
                } catch (IOException e) {
                    log.warn("Could not close the HDFS filesystem.", e);
                }
            }
        }
        super.close();
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        try {
            FileStatus status = fileSystem.getFileStatus(getFilePath(chunkName));
            return ChunkInfo.builder().name(chunkName).length(status.getLen()).build();
        } catch (IOException e) {
            throw convertException(chunkName, "doGetInfo", e);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        try {
            Path fullPath = getFilePath(chunkName);
            // Create the file, and then immediately close the returned OutputStream, so that HDFS may properly create the file.
            this.fileSystem.create(fullPath, READWRITE_PERMISSION, false, 0, this.config.getReplication(),
                    this.config.getBlockSize(), null).close();
            log.debug("Created '{}'.", fullPath);

            // return handle
            return ChunkHandle.writeHandle(chunkName);
        } catch (FileAlreadyExistsException e) {
            throw new ChunkAlreadyExistsException(chunkName, "HDFSChunkStorage::doCreate");
        } catch (IOException e) {
            throw convertException(chunkName, "doCreate", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        try {
            // Try accessing file.
            fileSystem.getFileStatus(getFilePath(chunkName));
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            throw convertException(chunkName, "checkExists", e);
        }
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        try {
            val path = getFilePath(handle.getChunkName());
            if (!this.fileSystem.delete(path, true)) {
                // File was not deleted. Check if exists.
                checkFileExists(handle.getChunkName(), "doDelete");
            }
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doDelete", e);
        }
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        checkFileExists(chunkName, "doOpenRead");
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        try {
            val status = fileSystem.getFileStatus(getFilePath(chunkName));
            if (status.getPermission().getUserAction() == FsAction.READ) {
                return ChunkHandle.readHandle(chunkName);
            } else {
                return ChunkHandle.writeHandle(chunkName);
            }
        } catch (IOException e) {
            throw convertException(chunkName, "doOpenWrite", e);
        }

    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset)
            throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        ensureInitializedAndNotClosed();
        try (FSDataInputStream stream = this.fileSystem.open(getFilePath(handle.getChunkName()))) {
            stream.readFully(fromOffset, buffer, bufferOffset, length);
        } catch (EOFException e) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                    "current size of chunk.", fromOffset));
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doRead", e);
        }
        return length;
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        try (FSDataOutputStream stream = this.fileSystem.append(getFilePath(handle.getChunkName()))) {
            if (stream.getPos() != offset) {
                // Looks like the filesystem changed from underneath us. This could be our bug, but it could be something else.
                throw new InvalidOffsetException(handle.getChunkName(), stream.getPos(), offset, "doWrite");
            }

            if (length == 0) {
                // Note: IOUtils.copyBytes with length == 0 will enter an infinite loop, hence the need for this check.
                return 0;
            }

            // We need to be very careful with IOUtils.copyBytes. There are many overloads with very similar signatures.
            // There is a difference between (InputStream, OutputStream, int, boolean) and (InputStream, OutputStream, long, boolean),
            // in that the one with "int" uses the third arg as a buffer size, and the one with "long" uses it as the number
            // of bytes to copy.
            IOUtils.copyBytes(data, stream, (long) length, false);

            stream.flush();
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doWrite", e);
        }
        return length;
    }

    @Override
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException {
        ensureInitializedAndNotClosed();
        int length = 0;
        try {
            val sources = new Path[chunks.length - 1];
            this.fileSystem.truncate(getFilePath(chunks[0].getName()), chunks[0].getLength());
            for (int i = 1; i < chunks.length; i++) {
                val chunkLength = chunks[i].getLength();
                this.fileSystem.truncate(getFilePath(chunks[i].getName()), chunkLength);
                sources[i - 1] = getFilePath(chunks[i].getName());
                length += chunkLength;
            }
            // Concat source file into target.
            this.fileSystem.concat(getFilePath(chunks[0].getName()), sources);
        } catch (IOException e) {
            throw convertException(chunks[0].getName(), "doConcat", e);
        }
        return length;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException {
        try {
            this.fileSystem.setPermission(getFilePath(handle.getChunkName()), isReadOnly ? READONLY_PERMISSION : READWRITE_PERMISSION);
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doSetReadOnly", e);
        }
    }

    @Override
    protected long doGetUsedSpace(OperationContext opContext) throws ChunkStorageException {
        try {
            return this.fileSystem.getUsed();
        } catch (IOException e) {
            throw convertException("", "doGetUsedSpace", e);
        }
    }

    //endregion

    //region Storage Implementation

    @SneakyThrows(IOException.class)
    public void initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem == null, "HDFSStorage has already been initialized.");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", this.config.getHdfsHostURL());
        conf.set("fs.default.fs", this.config.getHdfsHostURL());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        // We do not want FileSystem to cache clients/instances based on target URI.
        // This allows us to close instances without affecting other clients/instances. This should not affect performance.
        conf.set("fs.hdfs.impl.disable.cache", "true");
        if (!this.config.isReplaceDataNodesOnFailure()) {
            // Default is DEFAULT, so we only set this if we want it disabled.
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        }

        this.fileSystem = openFileSystem(conf);
        log.info("Initialized (HDFSHost = '{}'", this.config.getHdfsHostURL());
    }

    protected FileSystem openFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }
    //endregion

    //region Helpers
    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fileSystem != null, "HDFSStorage is not initialized.");
    }

    //endregion

    //Region HDFS helper methods.

    /**
     * Gets an HDFS-friendly path prefix for the given chunk name by pre-pending the HDFS root from the config.
     */
    private String getPathPrefix(String chunkName) {
        return this.config.getHdfsRoot() + Path.SEPARATOR + chunkName;
    }

    /**
     * Gets the full HDFS Path to a file for the given chunk, startOffset and epoch.
     */
    private Path getFilePath(String chunkName) {
        Preconditions.checkState(chunkName != null && chunkName.length() > 0, "chunkName must be non-null and non-empty");
        return new Path(getPathPrefix(chunkName));
    }

    private void checkFileExists(String chunkName, String message) throws ChunkStorageException {
        try {
            fileSystem.getFileStatus(getFilePath(chunkName));
        } catch (IOException e) {
            throw convertException(chunkName, message, e);
        }
    }

    private ChunkStorageException convertException(String chunkName, String message, IOException e) {
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        if (e instanceof RemoteException) {
            e = ((org.apache.hadoop.ipc.RemoteException) e).unwrapRemoteException();
        }
        if (e instanceof FileNotFoundException) {
            return new ChunkNotFoundException(chunkName, message, e);
        }
        if (e instanceof DiskChecker.DiskOutOfSpaceException || e instanceof QuotaExceededException) {
            return new ChunkStorageFullException(message, e);
        }
        return new ChunkStorageException(chunkName, message, e);
    }

    //endregion
}