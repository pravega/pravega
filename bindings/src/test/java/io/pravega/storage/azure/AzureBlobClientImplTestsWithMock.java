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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobClientBase;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AzureBlobClientImplTestsWithMock {
    @Test
    public void testExists() {
        val blobContainerClient = mock(BlobContainerClient.class);
        val blobClient = mock(BlobClient.class);
        val appendBlobClient = mock(AppendBlobClient.class);
        when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);
        when(blobClient.getAppendBlobClient()).thenReturn(appendBlobClient);

        when(appendBlobClient.exists()).thenReturn(false);

        val azureClient = new AzureBlobClientImpl(AzureStorageConfig.builder()
                .with(AzureStorageConfig.CONTAINER, "test")
                .build(), blobContainerClient);

        azureClient.exists("testBlob");

        verify(appendBlobClient).exists();

    }

    @Test
    public void testCreate() {
        val blobContainerClient = mock(BlobContainerClient.class);
        val blobClient = mock(BlobClient.class);
        val appendBlobClient = mock(AppendBlobClient.class);
        when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);
        when(blobClient.getAppendBlobClient()).thenReturn(appendBlobClient);

        val expected = new AppendBlobItem("", null, null, false, "",
                Integer.toString(0), 0);
        doReturn(expected).when(appendBlobClient).create(anyBoolean());

        val azureClient = new AzureBlobClientImpl(AzureStorageConfig.builder()
                .with(AzureStorageConfig.CONTAINER, "test")
                .build(), blobContainerClient);

        val actual = azureClient.create("testBlob");
        Assert.assertEquals(actual, expected);
        verify(appendBlobClient).create(false);

    }

    @Test
    public void testDelete() {
        val blobContainerClient = mock(BlobContainerClient.class);
        val blobClient = mock(BlobClient.class);
        val appendBlobClient = mock(AppendBlobClient.class);
        when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);
        when(blobClient.getAppendBlobClient()).thenReturn(appendBlobClient);

        val azureClient = new AzureBlobClientImpl(AzureStorageConfig.builder()
                .with(AzureStorageConfig.CONTAINER, "test")
                .build(), blobContainerClient);

        azureClient.delete("testBlob");

        verify(appendBlobClient).delete();
    }

    @Test
    public void testGetInputStream() {
        String blobName = "testBlobName";
        val blobContainerClient = mock(BlobContainerClient.class);
        val blobClient = mock(BlobClient.class);
        val blobClientBase = mock(BlobClientBase.class);
        when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);
        when(blobClientBase.getContainerClient()).thenReturn(blobContainerClient);

        val azureClient = new AzureBlobClientImpl(AzureStorageConfig.builder()
                .with(AzureStorageConfig.CONTAINER, "test")
                .build(), blobContainerClient);

        azureClient.getInputStream(blobName, 0, 10);
        blobContainerClient.getBlobClient(blobName);
        blobClientBase.openInputStream();
        blobClientBase.downloadContent();

        verify(blobClientBase).openInputStream();
    }

    @Test
    public void testAppendBlock() {
        val blobContainerClient = mock(BlobContainerClient.class);
        val blobClient = mock(BlobClient.class);
        val appendBlobClient = mock(AppendBlobClient.class);
        when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);
        when(blobClient.getAppendBlobClient()).thenReturn(appendBlobClient);

        val azureClient = new AzureBlobClientImpl(AzureStorageConfig.builder()
                .with(AzureStorageConfig.CONTAINER, "test")
                .build(), blobContainerClient);
        val inputStream = azureClient.getInputStream("testBlob", 0, 10);
        val conditions = mock(AppendBlobRequestConditions.class);

        azureClient.appendBlock("testBlob", 0, 10, inputStream);
        conditions.setAppendPosition(5L);

        verify(appendBlobClient).appendBlock(inputStream, 10);
    }

    @Test
    public void testGetBlobProperties() {
        val blobContainerClient = mock(BlobContainerClient.class);
        val blobClient = mock(BlobClient.class);
        when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);

        val azureClient = new AzureBlobClientImpl(AzureStorageConfig.builder()
                .with(AzureStorageConfig.CONTAINER, "test")
                .build(), blobContainerClient);

        azureClient.getBlobProperties("testBlob");

        verify(blobClient).getProperties();
    }
}
