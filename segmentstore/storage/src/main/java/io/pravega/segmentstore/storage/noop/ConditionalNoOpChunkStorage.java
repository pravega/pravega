package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import lombok.Getter;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

public class ConditionalNoOpChunkStorage implements ChunkStorage {

    @Getter
    final protected ChunkStorage inner;

    final protected NoOpChunkStorage noOpChunkStorage;

    public ConditionalNoOpChunkStorage(ChunkStorage inner, NoOpChunkStorage noOpChunkStorage) {
        this.inner = inner;
        this.noOpChunkStorage = noOpChunkStorage;
    }

    @Override
    public boolean supportsTruncation() {
        return inner.supportsTruncation();
    }

    @Override
    public boolean supportsAppend() {
        return inner.supportsAppend();
    }

    @Override
    public boolean supportsConcat() {
        return inner.supportsConcat();
    }

    /**
     * Checks for the existence of the chunk in the chunkStorage.
     * @param chunkName Name of the storage object to check.
     * @return boolean value depending on the chunk's existence
     */
    @Override
    public CompletableFuture<Boolean> exists(String chunkName) {
        return isMetadataCall(chunkName) ? inner.exists(chunkName) : noOpChunkStorage.exists(chunkName);
    }

    /**
     * Creates a chunk in the chunkStorage.
     * @param chunkName String name of the storage object to create.
     * @return handle to a chunk
     */
    @Override
    public CompletableFuture<ChunkHandle> create(String chunkName) {
        return isMetadataCall(chunkName) ? inner.create(chunkName) : noOpChunkStorage.create(chunkName);
    }

    @Override
    public CompletableFuture<ChunkHandle> createWithContent(String chunkName, int length, InputStream data) {
        return isMetadataCall(chunkName) ? inner.createWithContent(chunkName, length, data) : noOpChunkStorage.createWithContent(chunkName, length, data);
    }

    @Override
    public CompletableFuture<Void> delete(ChunkHandle handle) {
        return isMetadataCall(handle.getChunkName()) ? inner.delete(handle) : noOpChunkStorage.delete(handle);
    }

    @Override
    public CompletableFuture<ChunkHandle> openRead(String chunkName) {
        return isMetadataCall(chunkName) ? inner.openRead(chunkName) : noOpChunkStorage.openRead(chunkName);
    }

    @Override
    public CompletableFuture<ChunkHandle> openWrite(String chunkName) {
        return isMetadataCall(chunkName) ? inner.openWrite(chunkName) : noOpChunkStorage.openWrite(chunkName);
    }

    @Override
    public CompletableFuture<ChunkInfo> getInfo(String chunkName) {
        return isMetadataCall(chunkName) ? inner.getInfo(chunkName) : noOpChunkStorage.getInfo(chunkName);
    }

    @Override
    public CompletableFuture<Integer> read(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) {
        return isMetadataCall(handle.getChunkName()) ? inner.read(handle, fromOffset, length, buffer, bufferOffset) : noOpChunkStorage.read(handle, fromOffset, length, buffer, bufferOffset);
    }

    @Override
    public CompletableFuture<Integer> write(ChunkHandle handle, long offset, int length, InputStream data) {
        return isMetadataCall(handle.getChunkName()) ? inner.write(handle, offset, length, data) : noOpChunkStorage.write(handle, offset, length, data);
    }

    @Override
    public CompletableFuture<Integer> concat(ConcatArgument[] chunks) {
        return inner.concat(chunks);
    }

    @Override
    public CompletableFuture<Boolean> truncate(ChunkHandle handle, long offset) {
        return isMetadataCall(handle.getChunkName()) ? inner.truncate(handle, offset) : noOpChunkStorage.truncate(handle, offset);
    }

    @Override
    public CompletableFuture<Void> setReadOnly(ChunkHandle handle, boolean isReadonly) {
        return isMetadataCall(handle.getChunkName()) ? inner.setReadOnly(handle, isReadonly) : noOpChunkStorage.setReadOnly(handle, isReadonly);
    }

    @Override
    public CompletableFuture<Long> getUsedSpace() {
        return inner.getUsedSpace();
    }

    @Override
    public void report() {
        inner.report();
    }

    @Override
    public void close() throws Exception {
        if (this.inner != null) {
            this.inner.close();
        }
        if (noOpChunkStorage != null) {
            this.noOpChunkStorage.close();
        }
    }

    private boolean isMetadataCall(String chunkName) {
        return null != chunkName && chunkName.startsWith("_system");
    }
}
