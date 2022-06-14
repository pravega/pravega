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

import com.azure.storage.blob.specialized.AppendBlobClient;
import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.storage.chunklayer.*;
import io.pravega.storage.extendeds3.ExtendedS3StorageConfig;
import lombok.val;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;
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
        return null;
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        val blobItem = this.client.create(chunkName);
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        return client.exists(chunkName);
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        try {
            client.delete(handle.getChunkName());
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
            try (val inputStream = client.getInputStream(handle.getChunkName(), fromOffset, length)) {
                return StreamHelpers.readAll(inputStream, buffer, bufferOffset, length);
            }
        } catch (Exception e) {
                throw convertException(handle.getChunkName(), "doRead", e);
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        try {
            val metadata = client.getBlobProperties(handle.getChunkName(), length);
            if (metadata.getBlobSize() != offset) {
                throw new InvalidOffsetException(handle.getChunkName(), metadata.getBlobSize(), offset, "doWrite");
            }
            client.appendBlock(handle.getChunkName(), offset, length, data);
            return length;
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doWrite", e);
        }
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        return 0;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {

    }

    private ChunkStorageException convertException(String chunkName, String message, Exception e) {
        ChunkStorageException retValue = null;
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
//        if (e instanceof AzureException) {
//            AzureException azureException = (AzureException) e;
//            String errorCode = Strings.nullToEmpty(azureException.awsErrorDetails().errorCode());
//
//            if (errorCode.equals(NO_SUCH_KEY)) {
//                retValue = new ChunkNotFoundException(chunkName, message, e);
//            }
//
//            if (errorCode.equals(PRECONDITION_FAILED)) {
//                retValue = new ChunkAlreadyExistsException(chunkName, message, e);
//            }
//        }
        return retValue;
    }
}
