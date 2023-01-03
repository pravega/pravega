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
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link AzureBlobClientImpl} class provides implementation of {@link AzureClient} methods.
 */
@Slf4j
public class AzureBlobClientImpl implements AzureClient {

    /**
     * Client to a container which contains operations on the blob container.
     */
    private final BlobContainerClient blobContainerClient;

    /**
     * Providing different configurations for Azure Storage component.
     */
    private final AzureStorageConfig config;

    /**
     * Automatically updating the value to close the resources.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * The class provides implementation of the methods declared in AzureClient.
     * AzureBlobClientImpl constructor for initializing the following parameters and pre-checking the conditions.
     * @param config Configuration for Azure Storage Account.
     */
    public AzureBlobClientImpl(AzureStorageConfig config) {
        this.config = config;
        this.blobContainerClient = getBlobContainerClient(config);
        createContainerIfRequired(config, blobContainerClient);
    }

    /**
     * The class provides implementation of the methods declared in AzureClient.
     * AzureBlobClientImpl constructor for initializing the following parameters and pre-checking the conditions.
     * @param config Configuration for Azure Storage Account.
     * @param blobContainerClient client to a container performing operations on container.
     */
    public AzureBlobClientImpl(AzureStorageConfig config, BlobContainerClient blobContainerClient) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.blobContainerClient = blobContainerClient;
        createContainerIfRequired(config, blobContainerClient);
    }

    /**
     * Method to create container using the container client.
     * @param config Configuration for the Azure Storage component.
     * @param blobContainerClient Client to a container performing operations on container.
     */
    public void createContainerIfRequired(AzureStorageConfig config, BlobContainerClient blobContainerClient) {
        if (config.isCreateContainer()) {
            try {
                val containerProperties = blobContainerClient.getProperties();
            } catch (Exception e) {
                if (e instanceof BlobStorageException) {
                    BlobStorageException blobStorageException = (BlobStorageException) e;
                    val errorCode = blobStorageException.getErrorCode();
                    if (errorCode.equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
                        blobContainerClient.create();
                        log.debug("Creating container {}.", config.getContainerName());
                        return;
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Creates blob container client with the given config.
     * @param config Configuration for the Azure Storage component.
     * @return BlobContainerClient.
     */
    private BlobContainerClient getBlobContainerClient(AzureStorageConfig config) {
        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(config.getEndpoint())
                .credential(StorageSharedKeyCredential.fromConnectionString(config.getConnectionString()))
                .buildClient();
        val client = storageClient.getBlobContainerClient(config.getContainerName());
        return client;
    }

    @Override
    public AppendBlobItem create(String blobName) {
        Preconditions.checkState(!closed.get());
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        return appendBlobClient.create(false);
    }

    @Override
    public boolean exists(String blobName) {
        Preconditions.checkState(!closed.get());
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        return appendBlobClient.exists();
    }

    @Override
    public void delete(String blobName) {
        Preconditions.checkState(!closed.get());
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        appendBlobClient.delete();
    }

    @Override
    public InputStream getInputStream(String blobName, long offSet, long length) {
        Preconditions.checkState(!closed.get());
        BlobClientBase blobClientBase = blobContainerClient.getBlobClient(blobName);
        return blobClientBase.openInputStream(new BlobRange(offSet, length), new BlobRequestConditions());
    }

    @Override
    public AppendBlobItem appendBlock(String blobName, long offSet, long length, InputStream inputStream) {
        Preconditions.checkState(!closed.get());
        AppendBlobClient appendBlobClient = blobContainerClient.getBlobClient(blobName).getAppendBlobClient();
        val conditions = new AppendBlobRequestConditions();
        conditions.setAppendPosition(offSet);
        return appendBlobClient.appendBlock(inputStream, length);
    }

    @Override
    public BlobProperties getBlobProperties(String blobName) {
        Preconditions.checkState(!closed.get());
        val appendBlobClient = blobContainerClient.getBlobClient(blobName);
        return appendBlobClient.getProperties();
    }

    @Override
    @SneakyThrows
    public void close() throws Exception {
        this.closed.getAndSet(true);
    }
}
