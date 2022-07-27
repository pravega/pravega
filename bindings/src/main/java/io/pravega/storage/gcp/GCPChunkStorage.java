/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.RetryHelper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

/**
 * {@link ChunkStorage} for GCP (Google Cloud Platform) based storage.
 * <p>
 * Each chunk is represented as a single Object on the underlying storage.
 * <p>
 * This implementation works under the assumption that object is created only once and never modified.
 */
@Slf4j
public class GCPChunkStorage extends BaseChunkStorage {

    /**
     * Error code for not found in google cloud storage
     */
    private static final int FILE_NOT_FOUND = 404;

    //region members
    /**
     * GCPStorageConfig contains config values.
     */
    private final GCPStorageConfig config;

    /**
     * Storage is provided by GCP to perform cloud operation on google cloud storage.
     */
    private final Storage storage;

    //endregion

    //region constructor
    public GCPChunkStorage(Storage storage, GCPStorageConfig config, Executor executor) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.storage = Preconditions.checkNotNull(storage, "client");
    }
    //endregion

    //region capabilities

    @Override
    public boolean supportsConcat() {
        return false;
    }

    @Override
    public boolean supportsAppend() {
        return false;
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    //endregion

    //region implementation

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        if (!checkExists(chunkName)) {
            throw new ChunkNotFoundException(chunkName, "doOpenRead");
        }
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        if (!checkExists(chunkName)) {
            throw new ChunkNotFoundException(chunkName, "doOpenWrite");
        }
        return new ChunkHandle(chunkName, false);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
        try (ReadChannel readChannel = this.storage.reader(this.config.getBucket(), getObjectPath(handle.getChunkName()))) {
            try {
                readChannel.seek(fromOffset);
                readChannel.limit(fromOffset + length);
                int val = readChannel.read(ByteBuffer.wrap(buffer, bufferOffset, length));
                //Returns -1 if the channel has reached at the end of the stream.
                if (val == -1) {
                    ChunkInfo info = doGetInfo(handle.getChunkName());
                    if (fromOffset >= info.getLength() || (length + fromOffset) >= info.getLength()) {
                        throw new IllegalArgumentException(String.format("End of stream for chunk %s. fromOffset = %s, length = %s", handle.getChunkName(), fromOffset, length));
                    }
                }
                return val;
            } catch (Exception e) {
                throw convertException(handle.getChunkName(), "doRead", e);
            }
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) {
        throw new UnsupportedOperationException("GCPChunkStorage does not support writing to already existing objects.");
    }

    @Override
    public int doConcat(ConcatArgument[] chunks) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("GCP chunk storage does not support doConcat");
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("GCP chunk storage does not support doSetReadOnly");
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkNotFoundException {
        Blob blob = this.storage.get(this.config.getBucket(), getObjectPath(chunkName), Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
        if (null == blob) {
            throw new ChunkNotFoundException(chunkName, "doGetInfo", null);
        }
        return ChunkInfo.builder()
                .name(chunkName)
                .length(blob.getSize())
                .build();
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) {
        throw new UnsupportedOperationException("GCPChunkStorage does not support creating object without content.");
    }

    @Override
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        BlobId blobId = BlobId.of(this.config.getBucket(), getObjectPath(chunkName));
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
        try (InputStream inputStream = data) {
            this.storage.createFrom(blobInfo, inputStream);
            return ChunkHandle.writeHandle(chunkName);
        } catch (IOException e) {
            throw convertException(chunkName, "doCreateWithContent", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) {
        Page<Blob> blobs = this.storage.list(this.config.getBucket(), Storage.BlobListOption.prefix(getObjectPath(chunkName)), Storage.BlobListOption.pageSize(1));
        return blobs.getValues().iterator().hasNext();
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkNotFoundException {
        boolean deleted = this.storage.delete(this.config.getBucket(), getObjectPath(handle.getChunkName()));
        if (!deleted) {
            throw new ChunkNotFoundException(handle.getChunkName(), "doDelete", null);
        }
    }

    /**
     * This method takes exception instance and converts it to a meaningful exception.
     * @param chunkName - Name of the chunk
     * @param message - Error message
     * @param e - Exception
     * @return Takes the exception and converts it to meaningful exception
     */
    private ChunkStorageException convertException(String chunkName, String message, Exception e) {
        ChunkStorageException retValue = null;
        if (e instanceof IOException) {
            if (e.getCause() instanceof RetryHelper.RetryHelperException) {
                RetryHelper.RetryHelperException retryHelperException = (RetryHelper.RetryHelperException) e.getCause();
                if (retryHelperException.getCause() instanceof StorageException) {
                    StorageException storageException = (StorageException) retryHelperException.getCause();
                    retValue = getChunkNotFoundException(chunkName, message, storageException);
                }
            }
        } else if (e instanceof IllegalArgumentException) {
            throw (IllegalArgumentException) e;
        } else {
            retValue = new ChunkStorageException(chunkName, message, e);
        }

        return retValue;
    }

    /**
     * This method takes StorageException and converts it to ChunkNotFoundException.
     * @param chunkName - Name of the chunk
     * @param message - Error message
     * @param storageException - StorageException
     * @return Takes the StorageException and returns ChunkNotFoundException
     */
    private ChunkNotFoundException getChunkNotFoundException(String chunkName, String message, StorageException storageException) {
        int code = storageException.getCode();
        ChunkNotFoundException chunkNotFoundException = null;
        if (code == FILE_NOT_FOUND) {
            chunkNotFoundException = new ChunkNotFoundException(chunkName, message, storageException);
        }
        return chunkNotFoundException;
    }

    /**
     * This method takes object name and append a prefix to it.
     * @param objectName - The name of the object
     * @return returns the object name by appending prefix
     */
    private String getObjectPath(String objectName) {
        return config.getPrefix().concat(objectName);
    }
    //endregion
}
