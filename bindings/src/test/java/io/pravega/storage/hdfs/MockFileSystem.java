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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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
import javax.annotation.concurrent.GuardedBy;

/**
 * Mock FileSystem for use in unit tests.
 */
@ThreadSafe
class MockFileSystem extends FileSystem {
    //region Members

    private static final URI ROOT_URI = URI.create("");
    private final FileStatus root = new FileStatus(0, true, 1, 1, 1, 1, FsPermission.getDirDefault(), "", "", new Path("/"));
    @GuardedBy("files")
    private final HashMap<Path, FileData> files = new HashMap<>();
    @Setter
    private Function<Path, CustomAction> onCreate;
    @Setter
    private Function<Path, CustomAction> onDelete;

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
    public void concat(final Path trg, final Path[] sourcePaths) throws IOException {
        synchronized (this.files) {
            // Collect files first. This ensures that they all exist and that the args are valid, prior to making any changes.
            FileData target = getFileData(trg);
            val sources = new ArrayList<FileData>();
            val uniquePaths = new HashSet<String>();
            uniquePaths.add(trg.toString());
            for (Path sourcePath : sourcePaths) {
                Preconditions.checkArgument(uniquePaths.add(sourcePath.toString()),
                        "Circular dependency in concat arguments: file %s has been seen more than once.", sourcePath);

                val fd = getFileData(sourcePath);
                if (fd.contents.size() == 0) {
                    // There is a restriction in HDFS that empty files cannot be used as sources for concat...
                    throw new IOException(String.format("Source file '%s' is empty.", sourcePath));
                }

                sources.add(fd);
            }

            // Check target not readonly status.
            if (target.getStatus().getPermission().getUserAction() == FsAction.READ) {
                throw HDFSExceptionHelpers.segmentSealedException(trg.getName());
            }

            // Concatenate contents.
            for (FileData source : sources) {
                target.contents.write(source.contents.toByteArray());
            }

            // Delete sources.
            for (Path sourcePath : sourcePaths) {
                this.files.remove(sourcePath);
            }
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("rename is not allowed");
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
        if (f.equals(root.getPath())) {
            return root;
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
    public void setWorkingDirectory(Path newDir) {
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

        void removeAttribute(String name) throws IOException {
            synchronized (this.attributes) {
                if (this.attributes.remove(name) == null) {
                    throw new IOException("attribute '" + name + "' not set");
                }
            }
        }

        byte[] getAttribute(String name) throws IOException {
            synchronized (this.attributes) {
                byte[] data = this.attributes.get(name);
                if (data == null) {
                    throw new IOException("attribute '" + name + "' not set");
                }

                return data;
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

    class CreateNewFileAction extends CustomAction {
        CreateNewFileAction(Path target) {
            super(target);
        }

        @Override
        void execute() throws IOException {
            createInternal(this.target);
        }
    }

    class WaitAction extends MockFileSystem.CustomAction {
        private final CompletableFuture<Void> waitOn;

        WaitAction(Path target, CompletableFuture<Void> waitOn) {
            super(target);
            this.waitOn = waitOn;
        }

        @Override
        void execute() throws IOException {
            this.waitOn.join();
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

