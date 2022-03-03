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
package io.pravega.storage.extendeds3;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.request.PutObjectRequest;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageUnavailableException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import org.apache.http.HttpStatus;
import org.junit.Test;



import java.io.ByteArrayInputStream;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ExtendedS3ChunkStorage} using {@link org.mockito.Mockito}.
 */
public class ExtendedS3SimpleStorageMockTests extends ThreadPooledTestSuite {
    @Test
    public void testUnavailableException503() throws Exception {
        val status = HttpStatus.SC_SERVICE_UNAVAILABLE;
        testUnavailableException(status);
    }

    @Test
    public void testUnavailableException504() throws Exception {
        val status = HttpStatus.SC_GATEWAY_TIMEOUT;
        testUnavailableException(status);
    }

    private void testUnavailableException(int status) {
        val s3Client = mock(S3Client.class);
        val toThrow = new S3Exception("unavailable", status);
        doThrow(toThrow).when(s3Client).putObject(any(PutObjectRequest.class));
        val chunkStorage = new ExtendedS3ChunkStorage(s3Client,
                ExtendedS3StorageConfig.builder()
                        .with(ExtendedS3StorageConfig.CONFIGURI, "http://127.0.0.1?identity=x&secretKey=x")
                        .with(ExtendedS3StorageConfig.BUCKET, "test")
                        .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                        .with(ExtendedS3StorageConfig.USENONEMATCH, true)
                        .build(),
                executorService(), false, true);
        AssertExtensions.assertFutureThrows("Should throw an exception",
                chunkStorage.createWithContent("test", 10, new ByteArrayInputStream(new byte[10])),
                ex -> ex instanceof ChunkStorageUnavailableException);
    }

}
