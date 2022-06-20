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
import com.google.cloud.RetryHelper;
import com.google.cloud.storage.*;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

/**
 * {@link ChunkStorage} for GCP based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that is only created once and never modified.
 */
@Slf4j
public class GCPChunkStorage extends BaseChunkStorage {

    //GCP error codes
    private static  final int FILE_NOT_FOUND = 404;


//AWS error codes
    public static final String NO_SUCH_KEY = "NoSuchKey";
    public static final String PRECONDITION_FAILED = "PreconditionFailed";
    public static final String INVALID_RANGE = "InvalidRange";
    public static final String INVALID_ARGUMENT = "InvalidArgument";
    public static final String METHOD_NOT_ALLOWED = "MethodNotAllowed";
    public static final String ACCESS_DENIED = "AccessDenied";
    public static final String INVALID_PART = "InvalidPart";

    //region members
    private final GCPStorageConfig config;
    private final Storage storage;

    //endregion

    //region constructor
    public GCPChunkStorage(Storage storage, GCPStorageConfig config, Executor executor, boolean shouldCloseClient) {
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

        //Blob blob = this.storage.get(this.config.getBucket(), getObjectPath(chunkName), Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
        //try (ReadChannel readChannel = this.storage.reader(this.config.getBucket(), getObjectPath(handle.getChunkName()), new Storage.BlobSourceOption(StorageRpc.Option.START_OFF_SET, StorageRpc.Option.END_OFF_SET.value()))) {
        try (ReadChannel readChannel = this.storage.reader(this.config.getBucket(), getObjectPath(handle.getChunkName()))) {
            try {
                readChannel.seek(fromOffset);
                readChannel.limit(fromOffset + length);
                ByteBuffer b = ByteBuffer.wrap(buffer, bufferOffset, length);
                int val = readChannel.read(ByteBuffer.wrap(buffer, bufferOffset, length));
                if (val == -1) {
                    ChunkInfo info = doGetInfo(handle.getChunkName());
                    if (fromOffset >= info.getLength() || (length + fromOffset) >= info.getLength()) {
                        throw new IllegalArgumentException(handle.getChunkName());
                    }
                }
                return val;
            } catch (IOException e) {
                throw convertException(handle.getChunkName(), "doRead", e);
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
    public int doConcat(ConcatArgument[] chunks) {
        throw new UnsupportedOperationException("GCPChunkStorage does not concat.");
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkNotFoundException {
        if (!checkExists(handle.getChunkName())) {
            throw new ChunkNotFoundException(handle.getChunkName(), null, null);
        }
        // read only object
        // During object holds you can not delete or replace the object, but you can update metadata.
        BlobId blobId = BlobId.of(this.config.getBucket(), getObjectPath(handle.getChunkName()));
        this.storage.update(BlobInfo.newBuilder(blobId).setEventBasedHold(isReadOnly).build());
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkNotFoundException {
            Blob blob = this.storage.get(this.config.getBucket(), getObjectPath(chunkName), Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
            if (null == blob) {
                throw new ChunkNotFoundException(chunkName, null, null);
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
        byte[] bytes;
        try (InputStream inputStream = data){
            //bytes = inputStream.readAllBytes();
           // this.storage.create(blobInfo, bytes);
            this.storage.createFrom(blobInfo, data);
            //this.storage.create(blobInfo, data);
            return ChunkHandle.writeHandle(chunkName);
        } catch (IOException e) {
            throw convertException(chunkName, "doCreateWithContent", e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ChunkStorageException(e.getMessage(), null, null);
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
            throw new ChunkNotFoundException(handle.getChunkName(), null, null);
        }
    }
    private ChunkStorageException convertException(String chunkName, String message, Exception e) {
        ChunkStorageException retValue = null;
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        if (e instanceof IOException) {
            if (e.getCause() instanceof RetryHelper.RetryHelperException) {
                RetryHelper.RetryHelperException retryHelperException = (RetryHelper.RetryHelperException) e.getCause();
                if (retryHelperException.getCause() instanceof StorageException) {
                    StorageException storageException = (StorageException) retryHelperException.getCause();
                    int code = storageException.getCode();
                    if (code == FILE_NOT_FOUND) {
                        retValue = new ChunkNotFoundException(chunkName, message, e);
                    }
                }
            }
        }

        if (e instanceof IllegalArgumentException) {
            throw (IllegalArgumentException) e;
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
