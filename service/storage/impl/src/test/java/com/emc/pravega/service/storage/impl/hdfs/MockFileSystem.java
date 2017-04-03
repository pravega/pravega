/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.http.annotation.GuardedBy;

/**
 * Mock FileSystem for use in unit tests.
 */
@ThreadSafe
class MockFileSystem extends FileSystem {
    //region Members

    private static final URI ROOT_URI = URI.create("");
    private final FileStatus ROOT = new FileStatus(0, true, 1, 1, 1, 1, FsPermission.getDirDefault(), "", "", new Path("/"));
    @GuardedBy("files")
    private final HashMap<Path, FileData> files = new HashMap<>();
    @Setter
    private Function<Path, CustomAction> onCreate;
    @Setter
    private Function<Path, CustomAction> onDelete;
    @Setter
    private Function<Path, CustomAction> onRename;

    //endregion

    //region Constructor

    MockFileSystem() {
        setConf(new Configuration());
    }

    //endregion

    //region FileSystem Implementation

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        FSDataOutputStream result = new FSDataOutputStream(createInternal(f).contents, null);
        invokeCustomAction(this.onCreate, f);
        return result;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return new FSDataInputStream(new SeekableInputStream(getFileData(f).contents.toByteArray()));
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        FileData data = getFileData(f);
        if (data.getStatus().getPermission().getUserAction() == FsAction.READ) {
            throw HDFSExceptionHelpers.segmentSealedException(f.getName());
        }

        return new FSDataOutputStream(data.contents, null, data.contents.size());
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        synchronized (this.files) {
            if (this.files.getOrDefault(dst, null) != null) {
                throw new FileAlreadyExistsException(dst.getName());
            }

            FileData toRename = getFileData(src);
            this.files.put(dst, new FileData(dst, toRename));
            this.files.remove(src);
        }

        invokeCustomAction(this.onRename, src);
        return true;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        boolean result;
        synchronized (this.files) {
            result = this.files.remove(f) != null;
        }

        if (result) {
            invokeCustomAction(this.onDelete, f);
        }

        return result;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        if (f.equals(ROOT.getPath())) {
            return ROOT;
        }
        return getFileData(f).getStatus();
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        getFileData(p).setPermission(permission);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value) throws IOException {
        getFileData(path).setAttribute(name, value);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        return getFileData(path).getAttribute(name);
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        getFileData(path).removeAttribute(name);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        synchronized (this.files) {
            val rawResult = this.files.values().stream()
                                      .filter(fd -> fd.path.toString().startsWith(f.toString()))
                                      .map(FileData::getStatus)
                                      .collect(Collectors.toList());
            FileStatus[] result = new FileStatus[rawResult.size()];
            rawResult.toArray(result);
            return result;
        }
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
    }

    @Override
    public Path getWorkingDirectory() {
        return null;
    }

    @Override
    public URI getUri() {
        return ROOT_URI;
    }

    //endregion

    //region Other Methods and Properties

    int getFileCount() {
        synchronized (this.files) {
            return this.files.size();
        }
    }

    void clear() {
        synchronized (this.files) {
            this.files.clear();
        }
    }

    private FileData getFileData(Path f) throws IOException {
        synchronized (this.files) {
            FileData data = this.files.getOrDefault(f, null);
            if (data == null) {
                throw HDFSExceptionHelpers.segmentNotExistsException(f.getName());
            }

            return data;
        }
    }

    private void invokeCustomAction(Function<Path, CustomAction> actionCreator, Path target) throws IOException {
        if (actionCreator != null) {
            CustomAction action = actionCreator.apply(target);
            if (action != null) {
                action.execute();
            }
        }
    }

    private FileData createInternal(Path f) throws IOException {
        synchronized (this.files) {
            FileData data = this.files.getOrDefault(f, null);
            if (data != null) {
                throw new FileAlreadyExistsException(f.getName());
            }

            data = new FileData(f);
            this.files.put(f, data);
            return data;
        }
    }

    //endregion

    //region FileData

    @ThreadSafe
    private static class FileData {
        final Path path;
        final ByteArrayOutputStream contents = new ByteArrayOutputStream();
        @GuardedBy("contents")
        private FsPermission permission;
        @GuardedBy("attributes")
        private final HashMap<String, byte[]> attributes = new HashMap<>();

        FileData(Path path) {
            this.path = path;
            this.permission = FsPermission.getFileDefault();
        }

        @SneakyThrows(IOException.class)
        FileData(Path path, FileData source) {
            this.path = path;
            this.permission = source.permission;
            this.attributes.putAll(source.attributes);
            this.contents.write(source.contents.toByteArray());
        }

        void setAttribute(String name, byte[] data) {
            synchronized (this.attributes) {
                this.attributes.put(name, data);
            }
        }

        void removeAttribute(String name) {
            synchronized (this.attributes) {
                this.attributes.remove(name);
            }
        }

        byte[] getAttribute(String name) {
            synchronized (this.attributes) {
                return this.attributes.get(name);
            }
        }

        void setPermission(FsPermission permission) {
            synchronized (this.contents) {
                this.permission = permission;
            }
        }

        FileStatus getStatus() {
            synchronized (this.contents) {
                return new FileStatus(contents.size(), false, 1, 1, 1, 1, this.permission, "", "", path);
            }
        }

        @Override
        public String toString() {
            return String.format("%s (Length = %s)", this.path, this.contents);
        }
    }

    //endregion

    //region Custom Actions

    @RequiredArgsConstructor
    abstract class CustomAction {
        final Path target;

        abstract void execute() throws IOException;
    }

    class CreateNewFile extends CustomAction {
        CreateNewFile(Path target) {
            super(target);
        }

        @Override
        void execute() throws IOException {
            createInternal(this.target);
        }
    }

    class ThrowException extends CustomAction {
        private final Supplier<IOException> exceptionSupplier;

        ThrowException(Path target, Supplier<IOException> exceptionSupplier) {
            super(target);
            this.exceptionSupplier = exceptionSupplier;
        }

        @Override
        void execute() throws IOException {
            IOException ex = this.exceptionSupplier.get();
            if (ex != null) {
                throw ex;
            }
        }
    }

    //endregion

    //region SeekableInputStream

    /**
     * Enhances the ByteArrayInputStream class with Seekable and PositionedReadable, which are both required for reading
     * from the FileSystem.
     */
    private static class SeekableInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {

        SeekableInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void seek(long pos) throws IOException {
            super.pos = (int) pos;
        }

        @Override
        public long getPos() throws IOException {
            return super.pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            int oldPos = super.pos;
            seek(position);
            int bytesRead = super.read(buffer, offset, length);
            seek(oldPos);
            return bytesRead;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            int oldPos = super.pos;
            seek(position);
            super.read(buffer, offset, length);
            seek(oldPos);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            int oldPos = super.pos;
            seek(position);
            super.read(buffer);
            seek(oldPos);
        }
    }

    //endregion
}

