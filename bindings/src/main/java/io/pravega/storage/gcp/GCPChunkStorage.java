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
package io.pravega.storage.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.segmentstore.storage.chunklayer.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ChunkStorage} for GCP based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that is only created once and never modified.
 */
@Slf4j
public class GCPChunkStorage extends BaseChunkStorage {

    public static final String NO_SUCH_KEY = "NoSuchKey";
    public static final String PRECONDITION_FAILED = "PreconditionFailed";
    public static final String INVALID_RANGE = "InvalidRange";
    public static final String INVALID_ARGUMENT = "InvalidArgument";
    public static final String METHOD_NOT_ALLOWED = "MethodNotAllowed";
    public static final String ACCESS_DENIED = "AccessDenied";
    public static final String INVALID_PART = "InvalidPart";

    //region members
    private final GCPStorageConfig config;
    private final StorageOptions storageOptions;
    private final boolean shouldCloseClient;
    private final AtomicBoolean closed;

    //endregion

    //region constructor
    public GCPChunkStorage(StorageOptions storageOptions, GCPStorageConfig config, Executor executor, boolean shouldCloseClient) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.storageOptions = Preconditions.checkNotNull(storageOptions, "client");
        this.closed = new AtomicBoolean(false);
        this.shouldCloseClient = shouldCloseClient;
    }
    //endregion

    //region capabilities

    @Override
    public boolean supportsConcat() {
        return true;
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
        try (ReadChannel readChannel = this.storageOptions.getService().reader(this.config.getBucket(), getObjectPath(handle.getChunkName()))) {
            try {
                readChannel.seek(fromOffset);
                readChannel.setChunkSize(length);
                return readChannel.read(ByteBuffer.wrap(buffer));
            } catch (IOException e) {
                throw convertException(handle.getChunkName(), "doRead", e);
            }
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) {
        throw new UnsupportedOperationException("GCPChunkStorage does not support writing to already existing objects.");
    }

    @Override
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException {
        int totalBytesConcatenated = 0;
        return totalBytesConcatenated;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) {
        // read only bucket
        // 1 hr retention period
        Long retentionPeriod = 60 * 60L;
        BucketInfo bucketInfo = BucketInfo.newBuilder(this.config.getBucket()).setRetentionPeriod(retentionPeriod).build();
        this.storageOptions.getService().update(bucketInfo);

        // read only object
        // During object holds you can not delete or replace the object, but you can update metadata.
        BlobId blobId = BlobId.of(this.config.getBucket(), getObjectPath(handle.getChunkName()));
        this.storageOptions.getService().update(BlobInfo.newBuilder(blobId).setEventBasedHold(isReadOnly).build());
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        try {
            Blob blob = this.storageOptions.getService().get(this.config.getBucket(), chunkName, Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
            return ChunkInfo.builder()
                    .name(chunkName)
                    .length(blob.getSize())
                    .build();
        } catch (Exception e) {
            throw convertException(chunkName, "doGetInfo", e);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) {
        throw new UnsupportedOperationException("GCPChunkStorage does not support creating object without content.");
    }

    @Override
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        BlobId blobId = BlobId.of(getObjectPath(this.config.getBucket()), getObjectPath(chunkName));
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
        byte[] bytes;
        try (InputStream inputStream = data){
            bytes = inputStream.readAllBytes();
            this.storageOptions.getService().create(blobInfo, bytes);
            return ChunkHandle.writeHandle(chunkName);
        } catch (IOException e) {
            throw convertException(chunkName, "doCreateWithContent", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) {
        Page<Blob> blobs = this.storageOptions.getService().list(this.config.getBucket(), Storage.BlobListOption.prefix(getObjectPath(chunkName)), Storage.BlobListOption.pageSize(1));
        return blobs.getValues().iterator().hasNext();
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        try {
            this.storageOptions.getService().delete(this.config.getBucket(), getObjectPath(handle.getChunkName()));
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doDelete", e);
        }
    }

    @Override
    @SneakyThrows
    public void close() {
        if (shouldCloseClient && !this.closed.getAndSet(true)) {
            //this.client.close();
        }
        super.close();
    }

    /**
     * Create formatted string for range.
     */
    private String getRangeWithLength(long fromOffset, long length) {
        return String.format("bytes=%d-%d", fromOffset, fromOffset + length - 1);
    }

    private ChunkStorageException convertException(String chunkName, String message, Exception e) {
        ChunkStorageException retValue = null;
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        if (e instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) e;
            String errorCode = Strings.nullToEmpty(s3Exception.awsErrorDetails().errorCode());

            if (errorCode.equals(NO_SUCH_KEY)) {
                retValue = new ChunkNotFoundException(chunkName, message, e);
            }

            if (errorCode.equals(PRECONDITION_FAILED)) {
                retValue = new ChunkAlreadyExistsException(chunkName, message, e);
            }

            if (errorCode.equals(INVALID_RANGE)
                    || errorCode.equals(INVALID_ARGUMENT)
                    || errorCode.equals(METHOD_NOT_ALLOWED)
                    || s3Exception.awsErrorDetails().sdkHttpResponse().statusCode() == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
                throw new IllegalArgumentException(chunkName, e);
            }

            if (errorCode.equals(ACCESS_DENIED)) {
                retValue = new ChunkStorageException(chunkName, String.format("Access denied for chunk %s - %s.", chunkName, message), e);
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

    //endregion

}
