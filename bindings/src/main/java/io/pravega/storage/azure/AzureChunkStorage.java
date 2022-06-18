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
package io.pravega.storage.azure;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.base.Preconditions;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.storage.chunklayer.*;
import lombok.SneakyThrows;
import lombok.val;

import java.io.InputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class AzureChunkStorage extends BaseChunkStorage {
    private final AzureStorageConfig config;
    private final AzureClient client;
    private final boolean shouldClose;
    private final AtomicBoolean closed;
    private final boolean supportsAppend;

    public AzureChunkStorage(AzureClient client, AzureStorageConfig config, Executor executor, boolean supportsAppend, boolean shouldClose) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.client = Preconditions.checkNotNull(client, "client");
        this.closed = new AtomicBoolean(false);
        this.shouldClose = shouldClose;
        this.supportsAppend = supportsAppend;

    }
    @Override
    public boolean supportsTruncation() {
        return false;
    }

    @Override
    public boolean supportsAppend() {
        return supportsAppend;
    }

    @Override
    public boolean supportsConcat() {
        return false;
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        try {
            val blobProperties = client.getBlobProperties(getObjectPath(chunkName));
            return ChunkInfo.builder()
                    .name(chunkName)
                    .length(blobProperties.getBlobSize())
                    .build();
        } catch (Exception e) {
            throw convertException(chunkName, "doGetInfo", e);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        try {
            val blobItem = this.client.create(getObjectPath(chunkName));
            return ChunkHandle.writeHandle(chunkName);
        }catch (Exception e) {
            throw convertException(chunkName, "doCreate", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        return client.exists(getObjectPath(chunkName));
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        try {
            client.delete(getObjectPath(handle.getChunkName()));
        }catch (Exception e) {
            throw convertException(handle.getChunkName(), "doDelete", e);
        }
    }

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
        try  {
            try (val inputStream = client.getInputStream(getObjectPath(handle.getChunkName()), fromOffset, length)) {
                val retValue =  StreamHelpers.readAll(inputStream, buffer, bufferOffset, length);
                if (retValue == 0){
                    val blobProperties = client.getBlobProperties(getObjectPath(handle.getChunkName()));
                    if (blobProperties.getBlobSize() <= fromOffset || blobProperties.getBlobSize() <= fromOffset+length){
                        throw new IllegalArgumentException();
                    }
                }
                return retValue;
            }
        } catch (IllegalArgumentException e) {
                throw e;
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doRead", e);
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        try {
            val objectPath = getObjectPath(handle.getChunkName());
            val metadata = client.getBlobProperties(objectPath);
            if (metadata.getBlobSize() != offset) {
                throw new InvalidOffsetException(handle.getChunkName(), metadata.getBlobSize(), offset, "doWrite");
            }
            client.appendBlock(objectPath, offset, length, data);
            return length;
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doWrite", e);
        }
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        throw new UnsupportedOperationException("AzureChunkStorage does not support concat operation.");
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {
    }

    @Override
    @SneakyThrows
    public void close() {
        if (shouldClose && !this.closed.getAndSet(true)) {
            this.client.close();
        }
        super.close();
    }

    private ChunkStorageException convertException(String chunkName, String message, Exception e) {
        ChunkStorageException retValue = null;
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        if (e instanceof BlobStorageException) {
            BlobStorageException blobStorageException = (BlobStorageException) e;
            val errorCode = blobStorageException.getErrorCode();

            if (errorCode.equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                retValue = new ChunkNotFoundException(chunkName, message, e);
            }

            if (errorCode.equals(BlobErrorCode.BLOB_ALREADY_EXISTS)) {
                retValue = new ChunkAlreadyExistsException(chunkName, message, e);
            }

            if (errorCode.equals(BlobErrorCode.AUTHENTICATION_FAILED)) {
                retValue =  new ChunkStorageException(chunkName, String.format("Authentication failed for chunk %s - %s.", chunkName, message), e);
            }
        }

        if (retValue == null) {
            retValue = new ChunkStorageException(chunkName, message, e);
        }

        return retValue;
    }

    private String getObjectPath(String objectName) {
        return config.getPrefix() + objectName;
    }
}
