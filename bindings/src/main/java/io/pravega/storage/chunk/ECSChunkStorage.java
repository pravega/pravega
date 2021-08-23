package io.pravega.storage.chunk;

import com.google.common.base.Preconditions;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.storage.chunklayer.*;
import io.pravega.shared.NameUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.InputStream;
import java.util.concurrent.Executor;

import static software.amazon.awssdk.http.HttpStatusCode.*;

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
        this.bucket = config.getBucket();
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        try {
            var response = client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(getObjectPath(chunkName))
                    .build());
            log.info("head chunk {}", chunkName);
            if(!response.sdkHttpResponse().isSuccessful()){
                throw triageStatusCode(getObjectPath(chunkName), response.sdkHttpResponse().statusCode());
            }
            return ChunkInfo.builder().length(response.contentLength()).name(chunkName).build();
        } catch (Exception e){
            throw convertException(chunkName, "doGetInfo", e);
        }
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        try {
            var requestBuilder = PutObjectRequest
                    .builder()
                    .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                            .putHeader(config.HeaderEMCExtensionIndexGranularity, String.valueOf(config.indexGranularity))
                            .build());
            var response = client.putObject(requestBuilder.bucket(bucket)
                            .key(getObjectPath(chunkName)).contentLength(0L)
                            .build(),
                    RequestBody.empty());
            log.info("create chunk {}", chunkName);
            if (response.sdkHttpResponse().statusCode() == HttpStatusCode.OK) {
                return ChunkHandle.writeHandle(chunkName);
            } else {
                return ChunkHandle.writeHandle(chunkName); // TODO triage error code
            }
        } catch (Exception e) {
            throw convertException(chunkName, "doCreate", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        try {
            var response = client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(getObjectPath(chunkName))
                    .build());
            log.info("head chunk {}", chunkName);
            if(response.sdkHttpResponse().statusCode() == 404){
                return false;
            } else {
                if(!response.sdkHttpResponse().isSuccessful()){
                    throw triageStatusCode(getObjectPath(chunkName), response.sdkHttpResponse().statusCode());
                }
                return true;
            }
        } catch (Exception e) {
            if (e instanceof S3Exception) {
                S3Exception se = (S3Exception) e;
                if (se instanceof NoSuchKeyException){
                    return false;
                }
                if (se.statusCode() == 404) {
                    return false;
                }
            }
            throw convertException(chunkName, "checkExists", e);

        }
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        try {
            log.info("delete chunk {}", handle.getChunkName());
            var response = client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(getObjectPath(handle.getChunkName()))
                    .build());
            if(!response.sdkHttpResponse().isSuccessful()){
                throw triageStatusCode(getObjectPath(handle.getChunkName()), response.sdkHttpResponse().statusCode());
            }
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doCreate", e);
        }
    }

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws ChunkStorageException {
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected ChunkHandle doOpenWrite(String chunkName) throws ChunkStorageException {
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
        var responseIn = client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(getObjectPath(handle.getChunkName())).range(makeRange(fromOffset, fromOffset + length))
                .build());
        int bytesRead = 0;
        try {
            if (buffer != null) {
                bytesRead = StreamHelpers.readAll(responseIn, buffer, bufferOffset, length);
            } else {
                throw new ChunkNotFoundException(getObjectPath(handle.getChunkName()), "doRead");
            }
            log.info("read chunk {} with bytesRead {}", handle.getChunkName(), bytesRead);
            responseIn.close();
        } catch (Exception e) {
            throw convertException(handle.getChunkName(), "doRead", e);
        }

        return bytesRead;
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        try {
            var requestBuilder = PutObjectRequest
                    .builder()
                    .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                            .putHeader(config.HeaderEMCExtensionIndexGranularity, String.valueOf(config.indexGranularity))
                            .build());
            var response = client.putObject(requestBuilder.bucket(bucket)
                            .key(getObjectPath(handle.getChunkName())).contentLength((long) length)
                            .build(),
                    RequestBody.fromInputStream(data, length));
            log.info("write chunk {} with length {}", handle.getChunkName(), length);
            if(!response.sdkHttpResponse().isSuccessful()){
                throw triageStatusCode(getObjectPath(handle.getChunkName()), response.sdkHttpResponse().statusCode());
            }
            return length;
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
                            .putHeader(config.HeaderEMCExtensionIndexGranularity, String.valueOf(config.indexGranularity))
                            .build());
            var response = client.putObject(requestBuilder.bucket(bucket)
                            .key(getObjectPath(chunkName)).contentLength((long) length)
                            .build(),
                    RequestBody.fromInputStream(data, length));
            log.info("create chunk {} with length {}", chunkName, length);
            if(!response.sdkHttpResponse().isSuccessful()){
                throw triageStatusCode(getObjectPath(chunkName), response.sdkHttpResponse().statusCode());
            }
            return ChunkHandle.writeHandle(chunkName);
        } catch (Exception e) {
            throw convertException(chunkName, "doCreateWithContent", e);
        }
    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        int totalBytesConcatenated = 0;
        String targetPath = getObjectPath(chunks[0].getName());
        try {
            // check whether the target exists
            if (!checkExists(chunks[0].getName())) {
                throw new ChunkNotFoundException(chunks[0].getName(), "doConcat - Target segment does not exist");
            }
            // read from original chunks and copy to the first chunk
            for (int i = 1; i < chunks.length; i++) {
                if (0 != chunks[i].getLength()) {
                    var chunkHandle = chunks[i];
                    var requestBuilder = PutObjectRequest
                            .builder()
                            .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                                    .putHeader(config.HeaderEMCExtensionIndexGranularity, String.valueOf(config.indexGranularity))
                                    .build());
                    var headRes = client.headObject(HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(getObjectPath(chunkHandle.getName()))
                            .build());
                    long contentLength = headRes.contentLength();
                    var inputStream = client.getObject(GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(getObjectPath(chunkHandle.getName()))
                            .build());
                    var response = client.putObject(requestBuilder.bucket(bucket)
                                    .key(targetPath).contentLength(contentLength)
                                    .build(),
                            RequestBody.fromInputStream(inputStream, contentLength));
                    if(!response.sdkHttpResponse().isSuccessful()){
                        throw triageStatusCode(targetPath, response.sdkHttpResponse().statusCode());
                    }
                    totalBytesConcatenated += contentLength;
                }
            }
        } catch (RuntimeException e) {
            // Make spotbugs happy. Wants us to catch RuntimeException in a separate catch block.
            // Error message is REC_CATCH_EXCEPTION: Exception is caught when Exception is not thrown
            throw convertException(chunks[0].getName(), "doConcat", e);
        } catch (Exception e) {
            throw convertException(chunks[0].getName(), "doConcat", e);
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

    private ChunkStorageException convertException(String chunkName, String message, Exception e) {
        ChunkStorageException retValue = null;
        if (e instanceof ChunkStorageException) {
            return (ChunkStorageException) e;
        }
        if (e instanceof SdkException) {
            if (e instanceof NoSuchKeyException) {
                retValue = new ChunkNotFoundException(getObjectPath(chunkName), message, e);
            }
        }

        if (e instanceof S3Exception) {
            S3Exception se = (S3Exception) e;
            retValue = triageStatusCode(chunkName,  se.statusCode());
        }

        if (retValue == null) {
            retValue = new ChunkStorageException(getObjectPath(chunkName), message, e);
        }

        return retValue;
    }

    private ChunkStorageException triageStatusCode(String chunkName, int statusCode) {
        ChunkStorageException retValue = null;
        switch (statusCode){
            case BAD_REQUEST:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "Bad Request");
                break;
            case NOT_FOUND:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "Not Found");
                break;
            case UNAUTHORIZED:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "UNAUTHORIZED");
                break;
            case METHOD_NOT_ALLOWED:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "METHOD_NOT_ALLOWED");
                break;
            case FORBIDDEN:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "FORBIDDEN");
                break;
            case NOT_ACCEPTABLE:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "NOT_ACCEPTABLE");
                break;
            case REQUEST_TIMEOUT:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "REQUEST_TIMEOUT");
                break;
            case INTERNAL_SERVER_ERROR:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "INTERNAL_SERVER_ERROR");
                break;
            case BAD_GATEWAY:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "BAD_GATEWAY");
                break;
            case SERVICE_UNAVAILABLE:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "SERVICE_UNAVAILABLE");
                break;
            case GATEWAY_TIMEOUT:
                retValue = new ChunkStorageException(getObjectPath(chunkName), "GATEWAY_TIMEOUT");
                break;
            default:
                log.error("unknown error code {}", statusCode);
        }
        log.error("S3 request failed for chunk {} with statusCode {}", chunkName, statusCode);
        return retValue;
    }

    private String makeRange(long start, long end) {
        return "bytes=" + start + "-" + (end - 1);
    }

    private String getObjectPath(String objectName) {
        String chunkName = NameUtils.extractECSChunkIdFromChunkName(objectName);
        log.info("chunk name {} for object {}", chunkName, objectName);
        return chunkName;
    }

}
