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
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.val;

import java.io.InputStream;
import java.util.concurrent.Executor;

/**
 * {@link ChunkStorage} implementation that fails predictably based on provided list of {@link FlakinessPredicate}.
 */
public class FlakyChunkStorage extends BaseChunkStorage {
    /**
     * Wrapped {@link ChunkStorage} instance.
     */
    @Getter
    private final BaseChunkStorage innerChunkStorage;

    /**
     * {@link FlakyInterceptor} that intercepts calls and matches against given set of {@link FlakinessPredicate}.
     */
    @Getter
    private final FlakyInterceptor interceptor = new FlakyInterceptor();

    /**
     *  Constructs {@link FlakyChunkStorage} instance.
     *
     * @param innerChunkStorage {@link ChunkStorage instance to wrap}.
     * @param executor Executor to use.
     */
    public FlakyChunkStorage(BaseChunkStorage innerChunkStorage, Executor executor) {
        super(executor);
        this.innerChunkStorage = Preconditions.checkNotNull(innerChunkStorage, "innerChunkStorage");
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        // Apply any interceptors with identifier 'doWrite.before' or 'doWrite.after'
        interceptor.intercept(handle.getChunkName(), "doWrite.before");
        val ret = innerChunkStorage.doWrite(handle, offset, length, data);
        interceptor.intercept(handle.getChunkName(), "doWrite.after");
        return ret;
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        // Apply any interceptors with identifier 'doConcat.before' or 'doConcat.after'
        interceptor.intercept(chunks[0].getName(), "doConcat.before");
        val ret = innerChunkStorage.doConcat(chunks);
        interceptor.intercept(chunks[0].getName(), "doConcat.after");
        return ret;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {
        // Apply any interceptors with identifier 'doSetReadOnly.before' or 'doSetReadOnly.after'
        interceptor.intercept(handle.getChunkName(), "doSetReadOnly.before");
        innerChunkStorage.doSetReadOnly(handle, isReadOnly);
        interceptor.intercept(handle.getChunkName(), "doSetReadOnly.after");
    }

    @Override
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        // Apply any interceptors with identifier 'doWrite.before' or 'doWrite.after'
        interceptor.intercept(chunkName, "doWrite.before");
        // Make sure you are calling methods on super class.
        val handle = innerChunkStorage.doCreateWithContent(chunkName, length, data);
        interceptor.intercept(chunkName, "doWrite.after");
        return handle;
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        // Apply any interceptors with identifier 'checkExists.before' or 'checkExists.after'
        interceptor.intercept(chunkName, "checkExists.before");
        val ret = innerChunkStorage.checkExists(chunkName);
        interceptor.intercept(chunkName, "checkExists.after");
        return ret;
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        // Apply any interceptors with identifier 'doDelete.before' or 'doDelete.after'
        interceptor.intercept(handle.getChunkName(), "doDelete.before");
        innerChunkStorage.doDelete(handle);
        interceptor.intercept(handle.getChunkName(), "doDelete.after");
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        // Apply any interceptors with identifier 'doOpenRead.before' or 'doOpenRead.after'
        interceptor.intercept(chunkName, "doOpenRead.before");
        val ret = innerChunkStorage.doOpenRead(chunkName);
        interceptor.intercept(chunkName, "doOpenRead.after");
        return ret;
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        // Apply any interceptors with identifier 'doOpenWrite.before' or 'doOpenWrite.after'
        interceptor.intercept(chunkName, "doOpenWrite.before");
        val ret = innerChunkStorage.doOpenWrite(chunkName);
        interceptor.intercept(chunkName, "doOpenWrite.after");
        return ret;
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        // Apply any interceptors with identifier 'doGetInfo.before' or 'doGetInfo.after'
        interceptor.intercept(chunkName, "doGetInfo.before");
        val ret = innerChunkStorage.doGetInfo(chunkName);
        interceptor.intercept(chunkName, "doGetInfo.after");
        return ret;
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException, IllegalArgumentException {
        // Apply any interceptors with identifier 'doWrite.before' or 'doWrite.after'
        interceptor.intercept(chunkName, "doWrite.before");
        // Make sure you are calling methods on super class.
        val ret = innerChunkStorage.doCreate(chunkName);
        interceptor.intercept(chunkName, "doWrite.after");
        return ret;
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
        // Apply any interceptors with identifier 'doRead.before' or 'doRead.after'
        interceptor.intercept(handle.getChunkName(), "doRead.before");
        val ret = innerChunkStorage.doRead(handle, fromOffset, length, buffer, bufferOffset);
        interceptor.intercept(handle.getChunkName(), "doRead.after");
        return ret;
    }

    @Override
    public boolean supportsTruncation() {
        return innerChunkStorage.supportsTruncation();
    }

    @Override
    public boolean supportsAppend() {
        return innerChunkStorage.supportsAppend();
    }

    @Override
    public boolean supportsConcat() {
        return innerChunkStorage.supportsConcat();
    }
}
