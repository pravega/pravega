package io.pravega.storage.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.AppendBlobItem;
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
//        String testBlob = "testBlob";
        val blobContainerClient = mock(BlobContainerClient.class);
        val blobClient = mock(BlobClient.class);
        val appendBlobClient = mock(AppendBlobClient.class);
        val blobClientBase = mock(BlobClientBase.class);
        when(blobClientBase.getContainerClient()).thenReturn(blobContainerClient);
        when(blobContainerClient.getBlobClient(any())).thenReturn(blobClient);
        when(blobClient.getAppendBlobClient()).thenReturn(appendBlobClient);

        val azureClient = new AzureBlobClientImpl(AzureStorageConfig.builder()
                .with(AzureStorageConfig.CONTAINER, "test")
                .build(), blobContainerClient);

        azureClient.create("testBlobName");
//        Assert.assertTrue(h1.get);
        azureClient.getInputStream("testBlobName", 0, 10);
        blobClientBase.openInputStream(any(), any());
    }

}
