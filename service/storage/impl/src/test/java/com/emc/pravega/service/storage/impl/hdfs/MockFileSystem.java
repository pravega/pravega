/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
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
import org.apache.hadoop.util.Progressable;
import org.apache.http.annotation.GuardedBy;

/**
 * Mock FileSystem for use in unit tests.
 */
class MockFileSystem extends FileSystem {
    private final FileStatus ROOT = new FileStatus(0, true, 1, 1, 1, 1, FsPermission.getDirDefault(), "", "", new Path("/"));
    private final HashMap<Path, FileData> files = new HashMap<>();

    MockFileSystem(){
        setConf(new Configuration());
    }

    //region FileSystem Implementation

    @Override
    public URI getUri() {
        return URI.create("");
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return new FSDataInputStream(new ByteArrayInputStream(getFileData(f).contents.toByteArray()));
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        synchronized (this.files) {
            FileData data = this.files.getOrDefault(f, null);
            if (data != null) {
                throw new FileAlreadyExistsException(f.getName());
            }

            data = new FileData(f);
            this.files.put(f, data);
            return new FSDataOutputStream(data.contents, null);
        }
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        FileData data = getFileData(f);
        if (data.getStatus().getPermission().getUserAction() == FsAction.READ) {
            throw HDFSExceptionHelpers.segmentSealedException(f.getName());
        }

        return new FSDataOutputStream(data.contents, null);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        synchronized (this.files) {
            if (this.files.getOrDefault(dst, null) != null) {
                throw new FileAlreadyExistsException(dst.getName());
            }

            FileData toRename = getFileData(src);
            this.files.put(dst, toRename);
            this.files.remove(src);
            return true;
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        synchronized (this.files) {
            return this.files.remove(f) != null;
        }
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
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
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

    //endregion

    void clear(){
        synchronized (this.files){
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

        void setAttribute(String name, byte[] data) {
            synchronized (this.attributes) {
                this.attributes.put(name, data);
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
    }

    //endregion
}

