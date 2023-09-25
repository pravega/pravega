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

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobType;
import com.google.common.base.Preconditions;
import io.pravega.common.util.CollectionHelpers;
import lombok.val;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory mock for Azure client.
 */
public class MockAzureClient implements AzureClient {

    /**
     * Map of containerName to map of objectName to object data.
     */

    @GuardedBy("objects")
    private static final Map<String, Map<String, InMemoryBlobData>> DATA = new HashMap<>();

    AzureStorageConfig config;

    public MockAzureClient(AzureStorageConfig config) {
        this.config = config;
        if (!DATA.containsKey(config.getContainerName())) {
            DATA.put(config.getContainerName(), new HashMap<>());
        }
    }

    @Override
    synchronized public AppendBlobItem create(String blobName) {
        val objectMap = DATA.get(config.getContainerName());
        if (objectMap == null) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        if (objectMap.containsKey(blobName)) {
            throw new BlobStorageException("Container doesn't exist.", null, null);
        }
        objectMap.put(blobName, new InMemoryBlobData());
        return new AppendBlobItem("", null, null, false, "",
                Integer.toString(0), 0);
    }

    @Override
    synchronized public boolean exists(String blobName) {
        val objectMap = DATA.get(config.getContainerName());
        if (objectMap == null) {
            throw new BlobStorageException("Blob doesn't exist.", new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()),
                    BlobErrorCode.BLOB_NOT_FOUND.toString()), null);
        }
        return objectMap.containsKey(blobName);
    }

    @Override
    synchronized public void delete(String blobName) {
        val objectMap = DATA.get(config.getContainerName());
        if (objectMap == null) {
            throw new BlobStorageException("Container doesn't exist.", new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()),
                    BlobErrorCode.CONTAINER_NOT_FOUND.toString()), null);
        }
        if (!objectMap.containsKey(blobName)) {
            throw new BlobStorageException("Blob doesn't exist.", new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()),
                    BlobErrorCode.BLOB_NOT_FOUND.toString()), null);
        }
        objectMap.remove(blobName);
    }

    /**
     * Method to copy fixed bytes of data from the chunk and return the resulting byte[].
     * @param blobName Blob name.
     * @param offSetInBlob Blob offset position from which we need to start to copy the bytes.
     * @param length Length of the blob whose data is to be copied.
     * @return Input stream of byte[].
     */
    @Override
    synchronized public InputStream getInputStream(String blobName, long offSetInBlob, long length) {
        final InMemoryBlobData inMemoryBlobData = getBlobData(blobName);

        // Find chunk that contains data.
        int floorIndex = CollectionHelpers.findGreatestLowerBound(inMemoryBlobData.blockList,
                data -> Long.compare(offSetInBlob, data.start));
        if (inMemoryBlobData.blobLength < length || offSetInBlob > inMemoryBlobData.blobLength || inMemoryBlobData.blobLength < offSetInBlob + length) {
            throw new IllegalArgumentException();
        }
        // floorIndex -1 suggests offSetInBlob < data.start.
        if (floorIndex == -1) {
            throw new BlobStorageException("Container doesn't exist.", new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()),
                    BlobErrorCode.CONTAINER_NOT_FOUND.toString()), null);
        }

        byte[] retValue = new byte[Math.toIntExact(length)];
        int size = 0;
        int bytesCopied = 0;
        int srcIndex = 0;

        for (int i = floorIndex; i < inMemoryBlobData.blockList.size(); i++) {
            val currentBlock = inMemoryBlobData.blockList.get(i);
            val arrayToReadFrom = currentBlock.getData(); //byte[] to which data is copied equ to source
            val isLastBlock = currentBlock.getStart() + currentBlock.getLength() >= offSetInBlob + length;
            val isFirstBlock = currentBlock.getStart() <= offSetInBlob;
            if (isFirstBlock) {
                srcIndex = Math.toIntExact(offSetInBlob - currentBlock.getStart());
                if (isLastBlock) {
                    size = Math.toIntExact(length);
                } else {
                    size = Math.toIntExact(currentBlock.getStart() + currentBlock.getLength() - offSetInBlob);
                }
            } else {
                srcIndex = 0;
                if (isLastBlock) {
                    size = Math.toIntExact(length - bytesCopied);
                } else {
                    size = Math.toIntExact(currentBlock.getLength());
                }
            }
            Preconditions.checkState(size >= 0);
            Preconditions.checkState(length > bytesCopied);
            Preconditions.checkState(retValue.length >= size);
            System.arraycopy(arrayToReadFrom, srcIndex, retValue, bytesCopied, size);
            bytesCopied += size;
            if (isLastBlock) {
                break;
            }
        }
        return new ByteArrayInputStream(retValue);
    }


    @Override
    @SneakyThrows
    synchronized public AppendBlobItem appendBlock(String blobName, long offSet, long length, InputStream inputStream) {
        final InMemoryBlobData inMemoryBlobData = getBlobData(blobName);
        if (inMemoryBlobData.blobLength != offSet) {
            throw new BlobStorageException("Container doesn't exist.", new MockHttpResponse(404,
                    new HttpRequest(HttpMethod.HEAD, config.getEndpoint()), BlobErrorCode.SOURCE_CONDITION_NOT_MET.toString()), null);
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
        val objectMap = DATA.get(config.getContainerName());
        if (objectMap == null) {
            throw new BlobStorageException("Container doesn't exist.",
                    new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()), BlobErrorCode.BLOB_NOT_FOUND.toString()), null);
        }
        if (!objectMap.containsKey(blobName)) {
            throw new BlobStorageException("Blob doesn't exist.",
                    new MockHttpResponse(404, new HttpRequest(HttpMethod.HEAD, config.getEndpoint()),
                            BlobErrorCode.BLOB_NOT_FOUND.toString()), null);
        }
        val blobData = objectMap.get(blobName);

        return blobData;
    }

    @Override
    synchronized public BlobProperties getBlobProperties(String blobName) {
        final InMemoryBlobData inMemoryBlobData = getBlobData(blobName);
        return new BlobProperties(OffsetDateTime.now(), null, "",
                inMemoryBlobData.blobLength, "", null,
                "", "", "", "", null, BlobType.APPEND_BLOB,
                null, null, null, "", null, "", "", null,
                "", false, false, "", null, false,
                null, "", null, null, null);
    }

    @Override
    synchronized public void close() throws Exception {

    }

    @NotThreadSafe
    static class InMemoryBlobData {
        final List<InMemoryBlock> blockList = new ArrayList<>();
        long blobLength = 0;
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
        public static final String ERROR_CODE = "x-ms-error-code";
        final int statusCode;
        final String errorCode;

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
