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
package io.pravega.storage.s3;

import io.pravega.segmentstore.storage.chunklayer.ChunkStorageUnavailableException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import org.apache.http.HttpStatus;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link S3ChunkStorage} using {@link org.mockito.Mockito}.
 */
public class S3SimpleStorageMockTests extends ThreadPooledTestSuite {
    @Test
    public void testUnavailableException503() {
        testUnavailableException(HttpStatus.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testUnavailableException504() {
        testUnavailableException(HttpStatus.SC_GATEWAY_TIMEOUT);
    }

    private void testUnavailableException(int status) {
        val s3Client = mock(S3Client.class);
        val toThrow = S3Exception.builder()
                .statusCode(status)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .statusCode(status)
                                .build())
                        .build())
                .build();
        doThrow(toThrow).when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        val chunkStorage = new S3ChunkStorage(s3Client, S3StorageConfig.builder().build(), executorService(), false);
        AssertExtensions.assertFutureThrows("Should throw an exception",
                chunkStorage.createWithContent("test", 10, new ByteArrayInputStream(new byte[10])),
                ex -> ex instanceof ChunkStorageUnavailableException);
    }
}
