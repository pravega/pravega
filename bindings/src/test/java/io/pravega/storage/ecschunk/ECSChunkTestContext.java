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
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Test context Extended S3 tests.
 */
public class ECSChunkTestContext {
    private static final String BUCKET_NAME_PREFIX = "pravegatest-";
    final ECSChunkStorageConfig adapterConfig;
    public final List<S3Client> clients;
    public final int port;
    private final String configUri;


    public ECSChunkTestContext() {
        try {
            this.port = 9939;
            this.configUri = "http://10.245.128.121:" + port + ";" + "http://10.245.128.122:" + port;
            String bucketName = "pravega-bucket-ut";
            this.adapterConfig = ECSChunkStorageConfig.builder()
                    .with(ECSChunkStorageConfig.CONFIGURI, configUri)
                    .with(ECSChunkStorageConfig.BUCKET, bucketName)
                    .with(ECSChunkStorageConfig.PREFIX, "samplePrefix")
                    .build();

            clients = createS3Clients();
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private List<S3Client> createS3Clients() {
        List<S3Client> clients = new ArrayList<>();
        for (URI endpoint : adapterConfig.getEndpoints()) {
            clients.add(S3Client.builder()
                    .region(Region.EU_WEST_2)
                    .endpointOverride(endpoint)
                    .credentialsProvider(AnonymousCredentialsProvider.create())
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder()
                                    .numRetries(0)
                                    .build())
                            .build()).build());
        }
        return clients;
    }

    public void close() {
        if (clients != null) {
            clients.forEach(SdkAutoCloseable::close);
        }
    }
}