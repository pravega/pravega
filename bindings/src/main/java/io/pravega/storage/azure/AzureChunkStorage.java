package io.pravega.storage.azure;

import io.pravega.segmentstore.storage.chunklayer.*;

import java.io.InputStream;

public class AzureChunkStorage extends BaseChunkStorage {
    @Override
    public boolean supportsTruncation() {
        return false;
    }

    @Override
    public boolean supportsAppend() {
        return false;
    }

    @Override
    public boolean supportsConcat() {
        return false;
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        return null;
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        return null;
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        return false;
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {

    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        return null;
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        return null;
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
        return 0;
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        return 0;
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        return 0;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {

    }
}
