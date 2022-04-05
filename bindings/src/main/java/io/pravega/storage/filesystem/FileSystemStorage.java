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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AccessControlException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * Storage adapter for file system based storage.
 *
 * Each segment is represented as a single file on the underlying storage.
 *
 * Approach to fencing:
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * Each block of data has an offset assigned to it and Pravega always writes the same data to the same offset.
 *
 * With this assumption the only flow when a write call is made to the same offset twice is when ownership of the
 * segment changes from one host to another and both the hosts are writing to it.
 *
 * As write to same offset to a file is idempotent (any attempt to re-write data with the same file offset does not
 * cause any form of inconsistency), locking is not required.
 *
 * In the absence of locking this is the expected behavior in case of ownership change: both the hosts will keep
 * writing the same data at the same offset till the time the earlier owner gets a notification that it is not the
 * current owner. Once the earlier owner received this notification, it stops writing to the segment.
 */
@Slf4j
public class FileSystemStorage implements SyncStorage {
    //region members

    private final FileSystemStorageConfig config;
    private final AtomicBoolean closed;

    //endregion

    //region constructor

    /**
     * Creates a new instance of the FileSystemStorage class.
     *
     * @param config   The configuration to use.
     */
    public FileSystemStorage(FileSystemStorageConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.closed = new AtomicBoolean(false);

    }

    //endregion

    //region Storage implementation

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     *
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doOpenRead(streamSegmentName));
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        return execute(handle.getSegmentName(), () -> doRead(handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doGetStreamSegmentInfo(streamSegmentName));
    }

    @Override
    @SneakyThrows(StreamSegmentException.class)
    public boolean exists(String streamSegmentName) {
        return execute(streamSegmentName, () -> doExists(streamSegmentName));
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doOpenWrite(streamSegmentName));
    }

    @Override
    public SegmentHandle create(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doCreate(streamSegmentName));
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doWrite(handle, offset, data, length));
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doSeal(handle));
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doUnseal(handle));
    }

    @Override
    public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
        execute(targetHandle.getSegmentName(), () -> doConcat(targetHandle, offset, sourceSegment));
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doDelete(handle));
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support Segment truncation.");
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "OS_OPEN_STREAM", justification = "Rare operation. " +
            "The leaked object is collected by GC. In case of a iterator in a for loop this would be fast.")
    @Override
    public Iterator<SegmentProperties> listSegments() throws IOException {
        return Files.find(Paths.get(config.getRoot()),
                Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile())
                .map(path -> (SegmentProperties) getStreamSegmentInformation(config.getRoot(), path))
                .iterator();
    }

    @SneakyThrows
    private StreamSegmentInformation getStreamSegmentInformation(String root, Path path) {
        PosixFileAttributes attrs = Files.readAttributes(path.toAbsolutePath(), PosixFileAttributes.class);
        return StreamSegmentInformation.builder()
                .name(Paths.get(root).relativize(path).toString())
                .length(attrs.size())
                .sealed(!(attrs.permissions().contains(OWNER_WRITE)))
                .lastModified(new ImmutableDate(attrs.creationTime().toMillis()))
                .build();
    }

    @Override
    public SyncStorage withReplaceSupport() {
        return this.config.isReplaceEnabled() ? new FileSystemStorageWithReplace(this.config) : this;
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion

    @VisibleForTesting
    protected FileChannel getFileChannel(Path path, StandardOpenOption openOption) throws IOException {
        return FileChannel.open(path, openOption);
    }

    @VisibleForTesting
    protected long getFileSize(Path path) throws IOException {
        return Files.size(path);
    }

    //region private sync implementation

    protected SegmentHandle doOpenRead(String streamSegmentName) throws StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);
        Path path = getPath(streamSegmentName);

        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }

        LoggerHelpers.traceLeave(log, "openRead", traceId, streamSegmentName);
        return FileSystemSegmentHandle.readHandle(streamSegmentName);
    }

    protected SegmentHandle doOpenWrite(String streamSegmentName) throws StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        Path path = getPath(streamSegmentName);
        if (!Files.exists(path)) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        } else if (Files.isWritable(path)) {
            LoggerHelpers.traceLeave(log, "openWrite", traceId);
            return FileSystemSegmentHandle.writeHandle(streamSegmentName);
        } else {
            LoggerHelpers.traceLeave(log, "openWrite", traceId);
            return FileSystemSegmentHandle.readHandle(streamSegmentName);
        }
    }

    private int doRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getSegmentName(), offset, bufferOffset, length);
        Timer timer = new Timer();

        Path path = getPath(handle.getSegmentName());

        long fileSize = getFileSize(path);
        if (fileSize < offset) {
            throw new IllegalArgumentException(String.format("Reading at offset (%d) which is beyond the " +
                    "current size of segment (%d).", offset, fileSize));
        }

        try (FileChannel channel = getFileChannel(path, StandardOpenOption.READ)) {
            int totalBytesRead = 0;
            long readOffset = offset;
            do {
                ByteBuffer readBuffer = ByteBuffer.wrap(buffer, bufferOffset, length);
                int bytesRead = channel.read(readBuffer, readOffset);
                bufferOffset += bytesRead;
                totalBytesRead += bytesRead;
                length -= bytesRead;
                readOffset += bytesRead;
            } while (length != 0);
            FileSystemMetrics.READ_LATENCY.reportSuccessEvent(timer.getElapsed());
            FileSystemMetrics.READ_BYTES.add(totalBytesRead);
            LoggerHelpers.traceLeave(log, "read", traceId, totalBytesRead);
            return totalBytesRead;
        }
    }

    protected SegmentProperties doGetStreamSegmentInfo(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        PosixFileAttributes attrs = Files.readAttributes(Paths.get(config.getRoot(), streamSegmentName),
                PosixFileAttributes.class);
        StreamSegmentInformation information = StreamSegmentInformation.builder()
                .name(streamSegmentName)
                .length(attrs.size())
                .sealed(!(attrs.permissions().contains(OWNER_WRITE)))
                .lastModified(new ImmutableDate(attrs.creationTime().toMillis()))
                .build();

        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
        return information;
    }

    protected boolean doExists(String streamSegmentName) {
        return Files.exists(Paths.get(config.getRoot(), streamSegmentName));
    }

    protected SegmentHandle doCreate(String streamSegmentName) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);
        FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(FileSystemWrapper.READ_WRITE_PERMISSION);

        Path path = Paths.get(config.getRoot(), streamSegmentName);
        Path parent = path.getParent();
        assert parent != null;
        Files.createDirectories(parent);
        Files.createFile(path, fileAttributes);
        LoggerHelpers.traceLeave(log, "create", traceId);
        FileSystemMetrics.CREATE_COUNT.inc();
        return FileSystemSegmentHandle.writeHandle(streamSegmentName);
    }

    private Void doWrite(SegmentHandle handle, long offset, InputStream data, int length) throws IOException, StreamSegmentException {
        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getSegmentName(), offset, length);
        Timer timer = new Timer();

        if (handle.isReadOnly()) {
            throw new IllegalArgumentException("Write called on a readonly handle of segment " + handle.getSegmentName());
        }

        Path path = getPath(handle.getSegmentName());

        // Fix for the case where Pravega runs with super user privileges.
        // This means that writes to readonly files also succeed. We need to explicitly check permissions in this case.
        if (!isWritableFile(path)) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }

        long totalBytesWritten = 0;
        try (FileChannel channel = getFileChannel(path, StandardOpenOption.WRITE)) {
            long fileSize = channel.size();
            if (fileSize != offset) {
                throw new BadOffsetException(handle.getSegmentName(), fileSize, offset);
            }

            // Wrap the input data into a ReadableByteChannel, but do not close it. Doing so will result in closing
            // the underlying InputStream, which is not desirable if it is to be reused.
            ReadableByteChannel sourceChannel = Channels.newChannel(data);
            while (length != 0) {
                long bytesWritten = channel.transferFrom(sourceChannel, offset, length);
                assert bytesWritten > 0 : "Unable to make any progress transferring data.";
                offset += bytesWritten;
                totalBytesWritten += bytesWritten;
                length -= bytesWritten;
            }
            channel.force(false);
        }
        FileSystemMetrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
        FileSystemMetrics.WRITE_BYTES.add(totalBytesWritten);
        LoggerHelpers.traceLeave(log, "write", traceId);
        return null;
    }

    private boolean isWritableFile(Path path) throws IOException {
        PosixFileAttributes attrs = Files.readAttributes(path, PosixFileAttributes.class);
        return attrs.permissions().contains(OWNER_WRITE);
    }

    protected Void doSeal(SegmentHandle handle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle.getSegmentName());
        if (handle.isReadOnly()) {
            throw new IllegalArgumentException(handle.getSegmentName());
        }

        Files.setPosixFilePermissions(getPath(handle.getSegmentName()), FileSystemWrapper.READ_ONLY_PERMISSION);
        LoggerHelpers.traceLeave(log, "seal", traceId);
        return null;
    }

    private Void doUnseal(SegmentHandle handle) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "unseal", handle.getSegmentName());
        Files.setPosixFilePermissions(getPath(handle.getSegmentName()), FileSystemWrapper.READ_WRITE_PERMISSION);
        LoggerHelpers.traceLeave(log, "unseal", traceId);
        return null;
    }

    /**
     * Concatenation as currently implemented here requires that we read the data and write it back to target file.
     * We do not make the assumption that a native operation exists as this is not a common feature supported by file
     * systems. As such, a concatenation induces an important network overhead as each byte concatenated must be
     * read and written back when the storage is backed by a remote filesystem (through NFS).
     * <p>
     * This option was preferred as other option (of having one file per transaction) will result in server side
     * fragmentation and corresponding slowdown in cluster performance.
     *
     * @param targetHandle  Target Handle.
     * @param offset        Offset.
     * @param sourceSegment Source segment name.
     * @throws IOException If an exception occurred.
     */
    protected Void doConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws IOException {
        long traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle.getSegmentName(),
                offset, sourceSegment);

        Path sourcePath = getPath(sourceSegment);
        Path targetPath = getPath(targetHandle.getSegmentName());

        long length = getFileSize(sourcePath);
        try (FileChannel targetChannel = FileChannel.open(targetPath, StandardOpenOption.WRITE);
             RandomAccessFile sourceFile = new RandomAccessFile(String.valueOf(sourcePath), "r")) {
            if (isWritableFile(sourcePath)) {
                throw new IllegalStateException(String.format("Source segment (%s) is not sealed.", sourceSegment));
            }
            while (length > 0) {
                long bytesTransferred = targetChannel.transferFrom(sourceFile.getChannel(), offset, length);
                offset += bytesTransferred;
                length -= bytesTransferred;
            }
            targetChannel.force(false);
            Files.delete(sourcePath);
            LoggerHelpers.traceLeave(log, "concat", traceId);
            return null;
        }
    }

    protected Void doDelete(SegmentHandle handle) throws IOException {
        Files.delete(getPath(handle.getSegmentName()));
        return null;
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param segmentName Full name of the StreamSegment.
     * @param operation   The function to execute.
     * @param <R>         Return type of the operation.
     * @return Instance of the return type of the operation.
     * @throws StreamSegmentException If an exception occurred.
     */
    protected <R> R execute(String segmentName, Callable<R> operation) throws StreamSegmentException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        try {
            return operation.call();
        } catch (Exception e) {
            return throwException(segmentName, e);
        }
    }

    private <T> T throwException(String segmentName, Exception e) throws StreamSegmentException {
        if (isFileNotFoundException(e)) {
            throw new StreamSegmentNotExistsException(segmentName);
        }

        if (e instanceof FileAlreadyExistsException) {
            throw new StreamSegmentExistsException(segmentName);
        }

        if (e instanceof IndexOutOfBoundsException) {
            throw new IllegalArgumentException(e.getMessage());
        }

        if (e instanceof AccessControlException
                || e instanceof AccessDeniedException
                || e instanceof NonWritableChannelException) {
            throw new StreamSegmentSealedException(segmentName, e);
        }

        throw Exceptions.sneakyThrow(e);
    }

    protected boolean isFileNotFoundException(Exception e) {
        return e instanceof NoSuchFileException
                || e instanceof FileNotFoundException
                || e instanceof StreamSegmentNotExistsException;
    }

    protected Path getPath(String segmentName) {
        return Paths.get(config.getRoot(), segmentName);
    }

    //endregion

    //region FileSystemStorageWithReplace

    /**
     * {@link FileSystemStorage} implementation with "atomic" replace support.
     *
     * The {@link #replace} method works as follows:
     * 1. Creates a new temporary file and writes the replacement contents in it.
     * 2. Attempts to atomically rename (move) the temp file back into the original file. If unable to, deletes the original
     * file and then renames it.
     *
     * Since there is no guarantee that this operation is atomic, all methods have an auto-recovery built-in, which works
     * as follows:
     * 1. If the requested segment file does exist, everything works as in {@link FileSystemStorage}. Even if there exist
     * a temporary file, the sole existence of the base file indicates that we had an interrupted execution of {@link #replace},
     * so we cannot rely on that temporary file's existence.
     * 2. If the requested segment file does not exist, and there exists an associated temporary file, then the temporary
     * file is renamed back into the original file. This is safe because we only delete the original file after we have
     * fully written the temp file, so the latter contains all the data we wish to include.
     * 3. If neither the segment file nor the associated temp file exist, then a {@link StreamSegmentNotExistsException}
     * is thrown.
     */
    @VisibleForTesting
    static class FileSystemStorageWithReplace extends FileSystemStorage {
        @VisibleForTesting
        static final String TEMP_SUFFIX = ".replace.tmp";

        private FileSystemStorageWithReplace(FileSystemStorageConfig config) {
            super(config);
        }

        //region FileSystemStorage Overrides

        @Override
        protected SegmentHandle doCreate(String streamSegmentName) throws IOException {
            String tempSegmentName = getTempSegmentName(streamSegmentName);
            if (!super.doExists(streamSegmentName) && super.doExists(tempSegmentName)) {
                // Segment file does not exist, but temp one does. This represents an incomplete replace operation.
                // Finalize it now and report that the Segment does indeed exist.
                finalizeRename(getTempSegmentName(streamSegmentName), streamSegmentName);
                throw new FileAlreadyExistsException(streamSegmentName);
            }

            return super.doCreate(streamSegmentName);
        }

        @Override
        protected Void doDelete(SegmentHandle handle) throws IOException {
            // Clean up any incomplete replace leftovers.
            String tempSegmentName = getTempSegmentName(handle.getSegmentName());
            if (super.doExists(tempSegmentName)) {
                Files.delete(getPath(tempSegmentName));
                try {
                    return super.doDelete(handle);
                } catch (IOException ex) {
                    // It's OK if the segment file does not exist. That is likely the result of a partial replace.
                    return null;
                }
            }

            return super.doDelete(handle);
        }

        @Override
        protected SegmentHandle doOpenWrite(String streamSegmentName) throws StreamSegmentNotExistsException {
            return withRecovery(streamSegmentName, super::doOpenWrite);
        }

        @Override
        protected SegmentHandle doOpenRead(String streamSegmentName) throws StreamSegmentNotExistsException {
            return withRecovery(streamSegmentName, super::doOpenRead);
        }

        @Override
        protected SegmentProperties doGetStreamSegmentInfo(String streamSegmentName) throws IOException {
            return withRecovery(streamSegmentName, super::doGetStreamSegmentInfo);
        }

        @Override
        protected Void doConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws IOException {
            return withRecovery(sourceSegment, source -> super.doConcat(targetHandle, offset, source));
        }

        @Override
        protected boolean doExists(String streamSegmentName) {
            return super.doExists(streamSegmentName) || super.doExists(getTempSegmentName(streamSegmentName));
        }

        @Override
        public boolean supportsReplace() {
            return true;
        }

        @Override
        public void replace(@NonNull SegmentHandle segment, @NonNull BufferView contents) throws StreamSegmentException {
            String segmentName = segment.getSegmentName();
            execute(segment.getSegmentName(), () -> replaceExistingFile(segmentName, contents));
        }

        @Override
        public SyncStorage withReplaceSupport() {
            return this;
        }

        //endregion

        private <T, TEx extends Exception> T withRecovery(String segmentName, RecoverableAction<T, TEx> toExecute) throws TEx {
            try {
                return toExecute.apply(segmentName);
            } catch (Exception ex) {
                String tmpName = getTempSegmentName(segmentName);
                if (isFileNotFoundException(ex) && super.doExists(tmpName)) {
                    // Incomplete replace operation detected. Finish it up.
                    log.info("Incomplete replace operation detected for '{}'. Finalizing.", segmentName);
                    finalizeRename(tmpName, segmentName);

                    // Should be done now. Retry original operation.
                    log.debug("Replace finalized for '{}'. Retrying operation.", segmentName);
                    return toExecute.apply(segmentName);
                } else {
                    throw ex;
                }
            }
        }

        private Void replaceExistingFile(String segmentName, BufferView contents) throws IOException, StreamSegmentException {
            boolean baseExists = super.doExists(segmentName);
            boolean shouldReseal = baseExists && super.getStreamSegmentInfo(segmentName).isSealed();

            // Check temp file already exists.
            String tmpSegmentName = getTempSegmentName(segmentName);
            if (super.doExists(tmpSegmentName)) {
                if (baseExists) {
                    // We have both a temp file and a segment file. This is most likely the result of an incomplete replace,
                    // however we still have the original segment file around. It is safe to delete the temp file now.
                    log.info("Incomplete replace operation detected for '{}'. Deleting temp file before new replace attempt.", segmentName);
                    super.doDelete(super.doOpenWrite(tmpSegmentName));
                } else {
                    // Temp file exists, but the segment file does not. This may be the result of an incomplete replace,
                    // in which case we need to finalize that one to prevent deleting (what could be) the only persisted
                    // copy of our data.
                    log.info("Incomplete replace operation detected for '{}'. Finalizing before new replace attempt.", segmentName);
                    finalizeRename(tmpSegmentName, segmentName);
                }
            } else if (!baseExists) {
                throw new StreamSegmentNotExistsException(segmentName);
            }

            // Write given contents to temp file.
            val tmpHandle = super.doCreate(tmpSegmentName);
            try {
                super.doWrite(tmpHandle, 0, contents.getReader(), contents.getLength());
                if (shouldReseal) {
                    super.doSeal(tmpHandle);
                }
            } catch (Exception ex) {
                log.warn("Unable to write to temporary file when attempting to replace '{}'. Original file has not been touched. Cleaning up.",
                        segmentName, ex);
                super.doDelete(tmpHandle);
                throw ex;
            }

            // Sanity check #1 (only executes in tests).
            assert super.doGetStreamSegmentInfo(tmpSegmentName).getLength() == contents.getLength();

            // Rename file. After this is done, the replace operation is complete.
            finalizeRename(tmpSegmentName, segmentName);

            // Sanity check #2 (only executes in tests).
            assert super.doGetStreamSegmentInfo(segmentName).getLength() == contents.getLength();
            return null;
        }

        @SneakyThrows(StreamSegmentReplaceException.class)
        private void finalizeRename(String fromSegmentName, String toSegmentName) {
            val fromFile = getPath(fromSegmentName).toFile().getAbsoluteFile();
            val toFile = getPath(toSegmentName).toFile().getAbsoluteFile();
            boolean renamed = fromFile.renameTo(toFile);
            if (!renamed) {
                // File.renameTo does not guarantee atomic rename on certain file systems. If so, we can do this in a
                // two-step process by deleting the target file and then renaming the source file to the target.
                log.debug("File.renameTo unsuccessful for '{}' to '{}'. Attempting two-step replace.", fromSegmentName, toSegmentName);
                if (!toFile.delete() || !fromFile.renameTo(toFile)) {
                    throw new StreamSegmentReplaceException(fromSegmentName, toSegmentName);
                }
            }

            // Sanity check (only executes in tests).
            assert !super.doExists(fromSegmentName);
            log.debug("Renamed '{}' to '{}'.", fromSegmentName, toSegmentName);
        }

        private String getTempSegmentName(String segmentName) {
            assert !segmentName.endsWith(TEMP_SUFFIX);
            return segmentName + TEMP_SUFFIX;
        }

        private interface RecoverableAction<T, TEx extends Exception> {
            T apply(String segment) throws TEx;
        }

        /**
         * Exception that is thrown when a {@link SyncStorage#replace} method fails to execute due to the underlying
         * File System unable to perform the rename.
         */
        private static class StreamSegmentReplaceException extends StreamSegmentException {
            StreamSegmentReplaceException(String fromSegmentName, String toSegmentName) {
                super(toSegmentName, String.format("Could not rename temporary Segment '%s' to '%s'.", fromSegmentName, toSegmentName));
            }
        }
    }

    //endregion
}
