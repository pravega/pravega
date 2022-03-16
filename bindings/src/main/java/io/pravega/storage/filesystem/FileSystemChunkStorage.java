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
package io.pravega.storage.filesystem;

import com.google.common.base.Preconditions;
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
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * {@link ChunkStorage} for file system based storage.
 *
 * Each Chunk is represented as a single file on the underlying storage.
 * The concat operation is implemented as append.
 */

@Slf4j
public class FileSystemChunkStorage extends BaseChunkStorage {
    public static final String NO_SPACE_LEFT_ON_DEVICE = "No space left on device";
    //region members

    private final FileSystemStorageConfig config;

    private final FileSystemWrapper fileSystem;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the FileSystemChunkStorage class.
     *
     * @param config The configuration to use.
     * @param executor Executor for async operations.
     */
    public FileSystemChunkStorage(FileSystemStorageConfig config, Executor executor) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.fileSystem = new FileSystemWrapper();
    }

    /**
     * Creates a new instance of the FileSystemChunkStorage class.
     *
     * @param config The configuration to use.
     * @param fileSystem Object that wraps file system related calls.
     * @param executor Executor for a async operations.
     */
    public FileSystemChunkStorage(FileSystemStorageConfig config, FileSystemWrapper fileSystem, Executor executor) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.fileSystem = Preconditions.checkNotNull(fileSystem, "fileSystem");
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

    //region



    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        try {
            long chunkSize = fileSystem.getFileSize(getFilePath(chunkName));
            return ChunkInfo.builder()
                    .name(chunkName)
                    .length(chunkSize)
                    .build();
        } catch (IOException e) {
            throw  convertException(chunkName, "doGetInfo", e);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        try {
            FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);

            Path path = getFilePath(chunkName);
            Path parent = path.getParent();
            assert parent != null;
            fileSystem.createDirectories(parent);
            fileSystem.createFile(fileAttributes, path);

        } catch (IOException e) {
            throw convertException(chunkName, "doCreate", e);
        }

        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected boolean checkExists(String chunkName) {
        return fileSystem.exists(getFilePath(chunkName));
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        try {
            fileSystem.delete(getFilePath(handle.getChunkName()));
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doDelete", e);
        }
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        Path path = getFilePath(chunkName);

        if (!fileSystem.exists(path)) {
            throw new ChunkNotFoundException(chunkName, "doOpenRead");
        }
        if (!fileSystem.isRegularFile(path)) {
            throw new ChunkStorageException(chunkName, "doOpenRead - chunk is not a regular file.");
        }

        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        Path path = getFilePath(chunkName);
        if (!fileSystem.exists(path)) {
            throw new ChunkNotFoundException(chunkName, "doOpenWrite");
        }
        if (!fileSystem.isRegularFile(path)) {
            throw new ChunkStorageException(chunkName, "doOpenWrite - chunk is not a regular file.");
        }
        if (fileSystem.isWritable(path)) {
            return ChunkHandle.writeHandle(chunkName);
        } else {
            return ChunkHandle.readHandle(chunkName);
        }
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset)
            throws ChunkStorageException, NullPointerException, IndexOutOfBoundsException {
        Path path = getFilePath(handle.getChunkName());
        try {
            long fileSize = fileSystem.getFileSize(path);
            if (fileSize < fromOffset) {
                throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                        "current size of chunk (%d).", fromOffset, fileSize));
            }
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doRead", e);
        }

        try (FileChannel channel = fileSystem.getFileChannel(path, StandardOpenOption.READ)) {
            int totalBytesRead = 0;
            long readOffset = fromOffset;
            do {
                ByteBuffer readBuffer = ByteBuffer.wrap(buffer, bufferOffset, length);
                int bytesRead = channel.read(readBuffer, readOffset);
                bufferOffset += bytesRead;
                totalBytesRead += bytesRead;
                length -= bytesRead;
                readOffset += bytesRead;
            } while (length > 0);
            return totalBytesRead;
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doRead", e);
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        Path path = getFilePath(handle.getChunkName());

        long totalBytesWritten = 0;
        try (FileChannel channel = fileSystem.getFileChannel(path, StandardOpenOption.WRITE)) {
            long fileSize = channel.size();
            if (fileSize != offset) {
                throw new InvalidOffsetException(handle.getChunkName(), fileSize, offset, "doWrite");
            }

            // Wrap the input data into a ReadableByteChannel, but do not close it. Doing so will result in closing
            // the underlying InputStream, which is not desirable if it is to be reused.
            ReadableByteChannel sourceChannel = Channels.newChannel(data);
            while (length > 0) {
                long bytesWritten = channel.transferFrom(sourceChannel, offset, length);
                assert bytesWritten > 0 : "Unable to make any progress transferring data.";
                offset += bytesWritten;
                totalBytesWritten += bytesWritten;
                length -= bytesWritten;
            }
            channel.force(true);
        } catch (IOException e) {
            throw convertException(handle.getChunkName(), "doWrite", e);
        }
        return (int) totalBytesWritten;
    }

    @Override
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException {
        try {
            int totalBytesConcated = 0;
            Path targetPath = getFilePath(chunks[0].getName());
            long offset = chunks[0].getLength();
            try (val targetChannel = fileSystem.getFileChannel(targetPath, StandardOpenOption.WRITE)) {
                for (int i = 1; i < chunks.length; i++) {
                    val source = chunks[i];
                    Preconditions.checkArgument(!chunks[0].getName().equals(source.getName()), "target and source can not be same.");
                    Path sourcePath = getFilePath(source.getName());
                    long length = chunks[i].getLength();
                    Preconditions.checkState(offset <= fileSystem.getFileSize(targetPath));
                    Preconditions.checkState(length <= fileSystem.getFileSize(sourcePath));
                    try (val sourceChannel = fileSystem.getFileChannel(sourcePath, StandardOpenOption.READ)) {
                        while (length > 0) {
                            long bytesTransferred = targetChannel.transferFrom(sourceChannel, offset, length);
                            offset += bytesTransferred;
                            length -= bytesTransferred;
                        }
                        targetChannel.force(true);
                        totalBytesConcated += length;
                        offset += length;
                    }
                }
            }
            return totalBytesConcated;
        } catch (IOException e) {
            throw convertException(chunks[0].getName(), "doConcat", e);
        }
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException {
        Path path = null;
        try {
            path = getFilePath(handle.getChunkName());
            fileSystem.setPermissions(path, isReadOnly ? FileSystemWrapper.READ_ONLY_PERMISSION : FileSystemWrapper.READ_WRITE_PERMISSION);
        } catch (IOException e) {
            throw convertException(path.toString(), "doSetReadOnly", e);
        }
    }

    @Override
    protected long doGetUsedSpace(OperationContext opContext) throws ChunkStorageException  {
        try {
            return fileSystem.getUsedSpace(Paths.get(config.getRoot()));
        } catch (Exception e) {
            throw convertException("", "doGetUsedSpace", e);
        }
    }

    private ChunkStorageException convertException(String chunkName, String message, Exception e) {
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        ChunkStorageException toRet = null;
        if (e instanceof FileNotFoundException || e instanceof NoSuchFileException) {
            toRet = new ChunkNotFoundException(chunkName, message, e);
        } else if (e instanceof FileAlreadyExistsException) {
            toRet = new ChunkAlreadyExistsException(chunkName, message, e);
        } else if (e instanceof IOException && e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
            toRet = new ChunkStorageFullException(message, e);
        } else {
            toRet = new ChunkStorageException(chunkName, message, e);
        }
        return toRet;

    }

    private Path getFilePath(String chunkName) {
        return Paths.get(config.getRoot(), chunkName);
    }
    //endregion
}
