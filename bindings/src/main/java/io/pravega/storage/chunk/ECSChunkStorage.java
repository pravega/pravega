package io.pravega.storage.chunk;

import com.emc.object.Range;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.segmentstore.storage.chunklayer.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;

import static io.pravega.storage.chunk.ECSChunkStorageConfig.HeaderEMCExtensionIndexGranularity;
import static io.pravega.storage.chunk.ECSChunkStorageConfig.indexGranularity;
@Slf4j
public class ECSChunkStorage extends BaseChunkStorage {

    @NonNull
    private final ECSChunkStorageConfig config;
    @NonNull
    private final S3Client client;

    @NonNull
    private final String bucket;
    /**
     * Constructor.
     *
     * @param executor An Executor for async operations.
     */
    public ECSChunkStorage(S3Client client, ECSChunkStorageConfig config, Executor executor) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.client = Preconditions.checkNotNull(client, "client");
        this.bucket = ECSChunkStorageConfig.getBucket();
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        return null;
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        try {
            var requestBuilder = PutObjectRequest
                    .builder()
                    .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                            .putHeader(HeaderEMCExtensionIndexGranularity, String.valueOf(indexGranularity))
                            .build());
            var response = client.putObject(requestBuilder.bucket(bucket)
                            .key(chunkName).contentLength(0L)
                            .build(),
                    RequestBody.empty());
            if(response.sdkHttpResponse().statusCode() == HttpStatusCode.OK){
                return ChunkHandle.writeHandle(chunkName);
            } else {
                return ChunkHandle.writeHandle(chunkName);
            }
        } catch (Exception e) {
        throw convertException(chunkName, "doCreate", e);
    }
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        try {
            var responseIn = client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(chunkName)
                    .build());
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            } else {
                throw convertException(chunkName, "checkExists", e);
            }
        }
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
        var responseIn = client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(handle.getChunkName()).range(makeRange(fromOffset, fromOffset + length))
                .build());
        try {
            if (buffer != null) {
                var skipCount = bufferOffset;
                while (skipCount > 0) {
                    skipCount -= responseIn.skip(skipCount);
                }

                var cp = 0;
                var cl = buffer.length;
                while (cp < cl) {
                    var rc = responseIn.read(buffer, cp, cl - cp);
                    if (rc == -1) {
                        log.error("read bucket {} key {} did not fetch enough data {} bytes", bucket, handle.getChunkName(), buffer.length);
                        break;
                    }
                    cp += rc;
                }
            }
            responseIn.close();
        } catch (IOException e) {
            log.error("read bucket {} key {} response failed", bucket, handle.getChunkName(), e);
            return length;
        }

        return length;
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        try {
            var requestBuilder = PutObjectRequest
                    .builder()
                    .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                            .putHeader(HeaderEMCExtensionIndexGranularity, String.valueOf(indexGranularity))
                            .build());
            var response = client.putObject(requestBuilder.bucket(bucket)
                            .key(handle.getChunkName()).contentLength((long)length)
                            .build(),
                    RequestBody.fromInputStream(data, length));
            if(response.sdkHttpResponse().statusCode() == HttpStatusCode.OK){
                return length;
            } else {
                return length;
            }
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doWrite", e);
        }
    }

    @Override
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        try {
            var requestBuilder = PutObjectRequest
                    .builder()
                    .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                            .putHeader(HeaderEMCExtensionIndexGranularity, String.valueOf(indexGranularity))
                            .build());
            var response = client.putObject(requestBuilder.bucket(bucket)
                            .key(chunkName).contentLength((long)length)
                            .build(),
                    RequestBody.fromInputStream(data, length));
            if(response.sdkHttpResponse().statusCode() == HttpStatusCode.OK){
                return ChunkHandle.writeHandle(chunkName);
            } else {
                return ChunkHandle.writeHandle(chunkName);
            }
        } catch (Exception e) {
            throw convertException(chunkName, "doCreateWithContent", e);
        }
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        int totalBytesConcatenated = 0;
        String uploadId = null;
        String targetPath = getObjectPath(chunks[0].getName());
        boolean isCompleted = false;
        try {
            int partNumber = 1;

            SortedSet<MultipartPartETag> partEtags = new TreeSet<>();
            CreateMultipartUploadResponse response = client.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket(config.getBucket()).key(targetPath).build());
            uploadId = response.uploadId();
            // check whether the target exists
            if (!checkExists(chunks[0].getName())) {
                throw new ChunkNotFoundException(chunks[0].getName(), "doConcat - Target segment does not exist");
            }

            //Copy the parts
            for (int i = 0; i < chunks.length; i++) {
                if (0 != chunks[i].getLength()) {
                    val sourceHandle = chunks[i];
                    S3ObjectMetadata metadataResult = client.headObject(config.getBucket(),
                            getObjectPath(sourceHandle.getName()));
                    long objectSize = metadataResult.getContentLength(); // in bytes
                    Preconditions.checkState(objectSize >= chunks[i].getLength());
                    UploadPartCopyRequest copyRequest = UploadPartCopyRequest.builder().bucket(config.getBucket()).copySource(sourceHandle.getName()).()
                    CopyPartRequest copyRequest = new CopyPartRequest(config.getBucket(),
                            getObjectPath(sourceHandle.getName()),
                            config.getBucket(),
                            targetPath,
                            uploadId,
                            partNumber++).withSourceRange(Range.fromOffsetLength(0, chunks[i].getLength()));

                    CopyPartResult copyResult = client.uploadPartCopy(copyRequest);
                    partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));
                    totalBytesConcatenated += chunks[i].getLength();
                }
            }

            //Close the upload
            client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getBucket(),
                    targetPath, uploadId).withParts(partEtags));
            isCompleted = true;
        } catch (RuntimeException e) {
            // Make spotbugs happy. Wants us to catch RuntimeException in a separate catch block.
            // Error message is REC_CATCH_EXCEPTION: Exception is caught when Exception is not thrown
            throw convertException(chunks[0].getName(), "doConcat", e);
        } catch (Exception e) {
            throw convertException(chunks[0].getName(), "doConcat", e);
        } finally {
            if (!isCompleted && null != uploadId) {
                client.abortMultipartUpload(new AbortMultipartUploadRequest(config.getBucket(), targetPath, uploadId));
            }
        }
        return totalBytesConcatenated;
    }

    @Override
    protected void doSetReadOnly(ChunkHandle handle, boolean isReadOnly) throws ChunkStorageException, UnsupportedOperationException {

    }

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

    private ChunkStorageException convertException(String chunkName, String message, Exception e)  {
        ChunkStorageException retValue = null;
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        if (e instanceof com.emc.object.s3.S3Exception) {
            com.emc.object.s3.S3Exception s3Exception = (com.emc.object.s3.S3Exception) e;
            String errorCode = Strings.nullToEmpty(s3Exception.getErrorCode());

            if (errorCode.equals("NoSuchKey")) {
                retValue =  new ChunkNotFoundException(chunkName, message, e);
            }

            if (errorCode.equals("PreconditionFailed")) {
                retValue =  new ChunkAlreadyExistsException(chunkName, message, e);
            }

            if (errorCode.equals("InvalidRange")
                    || errorCode.equals("InvalidArgument")
                    || errorCode.equals("MethodNotAllowed")
                    || s3Exception.getHttpCode() == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
                throw new IllegalArgumentException(chunkName, e);
            }

            if (errorCode.equals("AccessDenied")) {
                retValue =  new ChunkStorageException(chunkName, String.format("Access denied for chunk %s - %s.", chunkName, message), e);
            }
        }

        if (retValue == null) {
            retValue = new ChunkStorageException(chunkName, message, e);
        }

        return retValue;
    }
    private String makeRange(long start, long end) {
        return "bytes=" + start + "-" + (end - 1);
    }
}
