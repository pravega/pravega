package io.pravega.storage.chunk;

import com.google.common.base.Preconditions;
import io.pravega.common.MathHelpers;
import io.pravega.segmentstore.storage.chunklayer.*;
import io.pravega.shared.NameUtils;
import io.pravega.storage.chunk.netty.NettyConnection;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static software.amazon.awssdk.http.HttpStatusCode.*;
@Slf4j
public class ECSChunkNettyStorage extends BaseChunkStorage{

    @NonNull
    private final ECSChunkStorageConfig config;
    @NonNull
    private final List<NettyConnection> nettyClients;

    private final AtomicInteger counter = new AtomicInteger(0);
    @NonNull
    private final String bucket;

    private final ConcurrentHashMap<String, NettyConnection> chunkClientMap = new ConcurrentHashMap<>();
    /**
     * Constructor.
     *
     * @param executor An Executor for async operations.
     */
    public ECSChunkNettyStorage(List<NettyConnection> nettyClients, ECSChunkStorageConfig config, Executor executor) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.nettyClients = Preconditions.checkNotNull(nettyClients, "client");
        this.bucket = config.getBucket();
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws ChunkStorageException {
        throw new UnsupportedOperationException("Netty connection is not support doGetInfo");
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws ChunkStorageException {
        throw new UnsupportedOperationException("Netty connection is not support doCreate");
    }

    protected ChunkHandle preCreate(String chunkName, S3Client client) throws ChunkStorageException {
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
            log.info("pre-create chunk {}", chunkName);
            if (response.sdkHttpResponse().statusCode() == HttpStatusCode.OK) {
                return ChunkHandle.writeHandle(chunkName);
            } else {
                return ChunkHandle.writeHandle(chunkName); // TODO triage error code
            }
        } catch (Exception e) {
            throw convertException(chunkName, "preCreate", e);
        }
    }

    @Override
    protected boolean checkExists(String chunkName) throws ChunkStorageException {
        throw new UnsupportedOperationException("Netty connection is not support checkExists");
    }

    @Override
    protected void doDelete(ChunkHandle handle) throws ChunkStorageException {
        throw new UnsupportedOperationException("Netty connection is not support doDelete");
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
        throw new UnsupportedOperationException("Netty connection is not support doRead");
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
        String chunkName = handle.getChunkName();
        try {
            boolean success = getClient(chunkName, false).putObject(bucket, chunkName, offset, length, data);
            if (success) return length;
        } catch (Exception e) {
            throw convertException(chunkName, "doWrite", e);
        }
        throw new ChunkStorageException(getObjectPath(chunkName), "doWrite");
    }

    @Override
    protected ChunkHandle doCreateWithContent(String chunkName, int length, InputStream data) throws ChunkStorageException {
        try {
            boolean success = getClient(chunkName, false).putObject(bucket, chunkName, 0, length, data);
            if (success) return ChunkHandle.writeHandle(chunkName);
        } catch (Exception e) {
            throw convertException(chunkName, "doCreateWithContent", e);
        }
        throw new ChunkStorageException(getObjectPath(chunkName), "doCreateWithContent");

    }

    @Override
    protected int doConcat(ConcatArgument[] chunks) throws ChunkStorageException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Netty connection is not support doConcat");
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

    private NettyConnection getClient(String chunk, boolean cache){
        NettyConnection nettyClient = chunkClientMap.get(chunk);
        if(nettyClient == null){
            NettyConnection newClient = getNextClient();
            newClient.connect();
            if (cache) {
                nettyClient = chunkClientMap.putIfAbsent(chunk, newClient);
                return Objects.requireNonNullElse(nettyClient, newClient);
            } else {
                return newClient;
            }
        }
        return nettyClient;
    }

    NettyConnection getNextClient() {
        return nettyClients.get(MathHelpers.abs(counter.incrementAndGet()) % nettyClients.size());
    }
}
