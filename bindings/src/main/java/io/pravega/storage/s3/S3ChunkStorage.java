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
package io.pravega.storage.s3;

import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkAlreadyExistsException;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import io.pravega.segmentstore.storage.chunklayer.ChunkNotFoundException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ConcatArgument;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.io.StreamHelpers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ChunkStorage} for S3 based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that is only created once and never modified.
 * The concat operation is implemented as multi part copy.
 */
@Slf4j
public class S3ChunkStorage extends BaseChunkStorage {

    public static final String NO_SUCH_KEY = "NoSuchKey";
    public static final String PRECONDITION_FAILED = "PreconditionFailed";
    public static final String INVALID_RANGE = "InvalidRange";
    public static final String INVALID_ARGUMENT = "InvalidArgument";
    public static final String METHOD_NOT_ALLOWED = "MethodNotAllowed";
    public static final String ACCESS_DENIED = "AccessDenied";
    public static final String INVALID_PART = "InvalidPart";

    //region members
    private final S3StorageConfig config;
    private final S3Client client;
    private final boolean shouldCloseClient;
    private final AtomicBoolean closed;

    //endregion

    //region constructor
    public S3ChunkStorage(S3Client client, S3StorageConfig config, Executor executor, boolean shouldCloseClient) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.client = Preconditions.checkNotNull(client, "client");
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
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(getObjectPath(handle.getChunkName()))
                    .range(getRangeWithLength(fromOffset, length))
                    .bucket(config.getBucket())
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = client.getObjectAsBytes(objectRequest);
            try (val inputStream = objectBytes.asInputStream()) {
                return StreamHelpers.readAll(inputStream, buffer, bufferOffset, length);
            }
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doRead", e);
        }
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) {
        throw new UnsupportedOperationException("S3ChunkStorage does not support writing to already existing objects.");
    }

    @Override
    public int doConcat(ConcatArgument[] chunks) throws ChunkStorageException {
        int totalBytesConcatenated = 0;
        String targetPath = getObjectPath(chunks[0].getName());
        String uploadId = null;
        boolean isCompleted = false;
        try {
            int partNumber = 1;

            val response = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                    .bucket(config.getBucket())
                    .key(targetPath)
                    .build());
            uploadId = response.uploadId();

            // check whether the target exists
            if (!checkExists(chunks[0].getName())) {
                throw new ChunkNotFoundException(chunks[0].getName(), "doConcat - Target segment does not exist");
            }
            CompletedPart[] completedParts = new CompletedPart[chunks.length];

            //Copy the parts
            for (int i = 0; i < chunks.length; i++) {
                if (0 != chunks[i].getLength()) {
                    val sourceHandle = chunks[i];
                    long objectSize = client.headObject(HeadObjectRequest.builder()
                            .bucket(this.config.getBucket())
                            .key(getObjectPath(sourceHandle.getName()))
                            .build()).contentLength();

                    Preconditions.checkState(objectSize >= chunks[i].getLength(),
                            "Length of object should be equal or greater. Length on LTS={} provided={}",
                            objectSize, chunks[i].getLength());

                    UploadPartCopyRequest copyRequest = UploadPartCopyRequest.builder()
                            .destinationBucket(config.getBucket())
                            .destinationKey(targetPath)
                            .sourceBucket(config.getBucket())
                            .sourceKey(getObjectPath(sourceHandle.getName()))
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .copySourceRange(getRangeWithLength(0, chunks[i].getLength()))
                            .build();
                    val copyResult = client.uploadPartCopy(copyRequest);
                    val eTag = copyResult.copyPartResult().eTag();

                    completedParts[i] = CompletedPart.builder()
                            .partNumber(partNumber)
                            .eTag(eTag)
                            .build();

                    partNumber++;
                    totalBytesConcatenated += chunks[i].getLength();
                }
            }

            //Close the upload
            CompletedMultipartUpload completedRequest = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();
            client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(config.getBucket())
                    .key(targetPath)
                    .multipartUpload(completedRequest)
                    .uploadId(uploadId)
                    .build());
            isCompleted = true;
        } catch (RuntimeException e) {
            // Make spotbugs happy. Wants us to catch RuntimeException in a separate catch block.
            // Error message is REC_CATCH_EXCEPTION: Exception is caught when Exception is not thrown
            throw convertException(chunks[0].getName(), "doConcat", e);
        } catch (Exception e) {
            throw convertException(chunks[0].getName(), "doConcat", e);
        } finally {
            if (!isCompleted && null != uploadId) {
                try {
                    client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                            .bucket(config.getBucket())
                            .key(targetPath)
                            .uploadId(uploadId)
                            .build());
                } catch (Exception e) {
                    throw convertException(chunks[0].getName(), "doConcat", e);
                }
            }
        }
        return totalBytesConcatenated;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException {
        try {
            setPermission(handle, isReadOnly ? Permission.READ : Permission.FULL_CONTROL);
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doSetReadOnly", e);
        }
    }

    private void setPermission(ChunkHandle handle, Permission permission) {
        throw new UnsupportedOperationException("S3ChunkStorage does not support ACL");
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        try {
            val objectPath = getObjectPath(chunkName);
            val response = client.headObject(HeadObjectRequest.builder()
                    .bucket(this.config.getBucket())
                    .key(objectPath)
                    .build());

            return ChunkInfo.builder()
                    .name(chunkName)
                    .length(response.contentLength())
                    .build();
        } catch (Exception e) {
            throw convertException(chunkName, "doGetInfo", e);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) {
        throw new UnsupportedOperationException("S3ChunkStorage does not support creating object without content.");
    }

    @Override
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        try {
            val objectPath = getObjectPath(chunkName);

            Map<String, String> metadata = new HashMap<>();
            metadata.put("Content-Type", "application/octet-stream");
            metadata.put("Content-Length", Integer.toString(length));
            val request = PutObjectRequest.builder()
                    .bucket(this.config.getBucket())
                    .key(objectPath)
                    .metadata(metadata)
                    .build();
            client.putObject(request, RequestBody.fromInputStream(data, length));

            return ChunkHandle.writeHandle(chunkName);
        } catch (Exception e) {
            throw convertException(chunkName, "doCreateWithContent", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        try {
            val objectPath = getObjectPath(chunkName);
            val response = client.headObject(HeadObjectRequest.builder()
                    .bucket(this.config.getBucket())
                    .key(objectPath)
                    .build());
            return true;
        } catch (S3Exception e) {
            if (e.awsErrorDetails().errorCode().equals(NO_SUCH_KEY)) {
                return false;
            } else {
                throw convertException(chunkName, "checkExists", e);
            }
        }
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        try {
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(this.config.getBucket())
                    .key(getObjectPath(handle.getChunkName()))
                    .build();
            client.deleteObject(deleteRequest);
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doDelete", e);
        }
    }

    @Override
    @SneakyThrows
    public void close() {
        if (shouldCloseClient && !this.closed.getAndSet(true)) {
            this.client.close();
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
