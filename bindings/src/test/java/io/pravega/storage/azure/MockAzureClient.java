package io.pravega.storage.azure;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import io.pravega.common.util.CollectionHelpers;
import lombok.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockAzureClient implements AzureClient {

    /**
     * Map of containerName to msp of objectName to object data.
     */

    @GuardedBy("objects")
    private static final Map<String, Map<String, InMemoryBlobData>> data = new HashMap<>();

    AzureStorageConfig config;

    public MockAzureClient(AzureStorageConfig config) {
        this.config = config;
        if(!data.containsKey(config.getContainerName())) {
            data.put(config.getContainerName(), new HashMap<>());
        }
    }

    @Override
    synchronized public AppendBlobItem create(String blobName) {
        val objectMap = data.get(config.getContainerName());
        if(objectMap == null) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        if(objectMap.containsKey(blobName)) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        objectMap.put(blobName, new InMemoryBlobData());
        return new AppendBlobItem("", null, null, false, "",
                Integer.toString(0), 0);
    }

    @Override
    synchronized public boolean exists(String blobName) {
        val objectMap = data.get(config.getContainerName());
        if(objectMap == null) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        return objectMap.containsKey(blobName);
    }

    @Override
    synchronized public void delete(String blobName) {
        val objectMap = data.get(config.getContainerName());
        if(objectMap == null) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        if(objectMap.containsKey(blobName)) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        objectMap.remove(blobName);
    }

    @Override
    synchronized public InputStream getInputStream(String blobName, long offSetInBlob, long length) {
        final InMemoryBlobData inMemoryBlobData = getBlobData(blobName);

        // Find chunk that contains data.
        int floorIndex = CollectionHelpers.findGreatestLowerBound(inMemoryBlobData.blockList,
                data -> Long.compare(offSetInBlob, data.start));
        if (floorIndex == -1) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }

        byte[] retValue = new byte[Math.toIntExact(length)];
        for(int i = floorIndex; i < inMemoryBlobData.blockList.size(); i++) {
            val currentBlock = inMemoryBlobData.blockList.get(i);
            val arrayToReadFrom = currentBlock.getData();
            //start and end offset in array to copy
            //how to exit for loop
        }


        return new ByteArrayInputStream(retValue);
    }

    @Override
    @SneakyThrows
    synchronized public AppendBlobItem appendBlock(String blobName, long offSet, long length, InputStream inputStream) {
        final InMemoryBlobData inMemoryBlobData = getBlobData(blobName);
        if(inMemoryBlobData.blobLength != offSet) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        inMemoryBlobData.blockList.add(InMemoryBlock.builder()
                .data(inputStream.readAllBytes())
                .start(inMemoryBlobData.blobLength)
                .length(length)
                .build());
        inMemoryBlobData.blobLength += length;
        return new AppendBlobItem("", null, null, false, "",
                Integer.toString(Math.toIntExact(inMemoryBlobData.blobLength)), inMemoryBlobData.blockList.size());
    }

    private InMemoryBlobData getBlobData(String blobName) {
        val objectMap = data.get(config.getContainerName());
        if(objectMap == null) {
            throw new BlobStorageException("Container doesn't exist.",
                    new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()),
                            ""), null);
        }
        if(!objectMap.containsKey(blobName)) {
            throw new BlobStorageException("Container doesn't exist.",
                    new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()),
                            BlobErrorCode.BLOB_NOT_FOUND.toString()), null);
        }
        val blobData = objectMap.get(blobName);

        return blobData;
    }

    @Override
    synchronized public BlobProperties getBlobProperties(String blobName) {
        final InMemoryBlobData inMemoryBlobData = getBlobData(blobName);
        return null;
//        return new BlobProperties();
    }

    @Override
    synchronized public void close() throws Exception {

    }

    @NotThreadSafe
    static class InMemoryBlobData {
        final List<InMemoryBlock> blockList = new ArrayList<>();
        volatile long blobLength = 0;
    }


    @Builder
    static class InMemoryBlock {
        @Getter
        @Setter
        long start;

        @Getter
        @Setter
        long length;

        @Getter
        @Setter
        byte[] data;
    }

    static class MockHttpResponse extends HttpResponse {
        final int statusCode;
        final String errorCode;
        public static final String ERROR_CODE = "x-ms-error-code";


        public MockHttpResponse(int statusCode, HttpRequest httpRequest, String errorCode) {
            super(httpRequest);
            this.statusCode = statusCode;
            this.errorCode = errorCode;
        }

        @Override
        public int getStatusCode() {
            return statusCode;
        }

        @Override
        public String getHeaderValue(String name) {
            return null;
        }

        @Override
        public HttpHeaders getHeaders() {
            val headers = new HttpHeaders();
            headers.set(ERROR_CODE, errorCode);
            return headers;
        }

        @Override
        public Flux<ByteBuffer> getBody() {
            return null;
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return null;
        }

        @Override
        public Mono<String> getBodyAsString() {
            return null;
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset) {
            return null;
        }
    }
}
