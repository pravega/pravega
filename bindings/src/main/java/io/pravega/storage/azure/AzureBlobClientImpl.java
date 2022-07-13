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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.common.StorageSharedKeyCredential;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AzureBlobClientImpl class provides implementation of AzureClient methods.
 */
@Slf4j
public class AzureBlobClientImpl implements AzureClient {
    private final BlobContainerClient blobContainerClient;
    private final AzureStorageConfig config;
    private final boolean shouldCloseClient;
    private final AtomicBoolean closed;
    private final AzureClient client;

    public AzureBlobClientImpl(AzureStorageConfig config) {
        this.config = config;
        this.blobContainerClient = getBlobContainerClient(config);
        createContainerIfRequired(config, blobContainerClient);
        this.shouldCloseClient = false;
        this.closed = new AtomicBoolean(false);
        this.client = null;
    }

    public AzureBlobClientImpl(AzureStorageConfig config, BlobContainerClient blobContainerClient) {
        this.config = config;
        this.blobContainerClient = blobContainerClient;
        createContainerIfRequired(config, blobContainerClient);
        shouldCloseClient = false;
        closed = null;
        client = null;
    }

    public void createContainerIfRequired(AzureStorageConfig config, BlobContainerClient blobContainerClient) {
        log.debug("Creating container {}.", config.getContainerName());
        if (config.isCreateContainer()) {
            try {
                val containerProperties = blobContainerClient.getProperties();
            } catch (Exception e) {
                if (e instanceof BlobStorageException) {
                    BlobStorageException blobStorageException = (BlobStorageException) e;
                    val errorCode = blobStorageException.getErrorCode();
                    if (errorCode.equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
                        blobContainerClient.create();
                        return;
                    }
                }
                throw e;
            }
        }
    }

    private BlobContainerClient getBlobContainerClient(AzureStorageConfig config) {
        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(config.getEndpoint())
                .credential(StorageSharedKeyCredential.fromConnectionString(config.getConnectionString()))
                .buildClient();
        val containerClient = storageClient.getBlobContainerClient(config.getContainerName());
        return containerClient;
    }

    @Override
    public AppendBlobItem create(String blobName) {
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        return appendBlobClient.create(false);
    }

    @Override
    public boolean exists(String blobName) {
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        return appendBlobClient.exists();
    }

    @Override
    public void delete(String blobName) {
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        appendBlobClient.delete();
    }

    @Override
    public InputStream getInputStream(String blobName, long offSet, long length) {
        BlobClientBase blobClientBase = blobContainerClient.getBlobClient(blobName);
        return blobClientBase.openInputStream(new BlobRange(offSet, length), new BlobRequestConditions());
    }

    @Override
    public AppendBlobItem appendBlock(String blobName, long offSet, long length, InputStream inputStream) {
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        val conditions = new AppendBlobRequestConditions();
        conditions.setAppendPosition(offSet);
        return appendBlobClient.appendBlock(inputStream, length);
    }

    @Override
    public BlobProperties getBlobProperties(String blobName) {
        val appendBlobClient = blobContainerClient.getBlobClient(blobName);
        return appendBlobClient.getProperties();
    }

    @Override
    @SneakyThrows
    public void close() throws Exception {
        if (shouldCloseClient && !this.closed.getAndSet(true)) {
            this.client.close();
        }
    }
}
