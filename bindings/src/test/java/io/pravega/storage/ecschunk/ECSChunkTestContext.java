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
package io.pravega.storage.ecschunk;

import io.pravega.storage.chunk.ECSChunkStorageConfig;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Test context Extended S3 tests.
 */
public class ECSChunkTestContext {
    private static final String BUCKET_NAME_PREFIX = "pravegatest-";
    final ECSChunkStorageConfig adapterConfig;
    public final S3Client client;
    public final int port;
    private final String configUri;


    public ECSChunkTestContext() {
        try {
            this.port = 9939;
            this.configUri = "http://10.245.128.121:" + port;
            String bucketName = "pravega-bucket-ut";
            this.adapterConfig = ECSChunkStorageConfig.builder()
                    .with(ECSChunkStorageConfig.CONFIGURI, configUri)
                    .with(ECSChunkStorageConfig.BUCKET, bucketName)
                    .with(ECSChunkStorageConfig.PREFIX, "samplePrefix")
                    .build();
            client = S3Client.builder()
                    .region(Region.EU_WEST_2)
                    .endpointOverride(adapterConfig.getEndpoint())
                    .credentialsProvider(AnonymousCredentialsProvider.create())
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder()
                                    .numRetries(0)
                                    .build())
                            .build()).build();
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}