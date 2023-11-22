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

import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.test.common.TestUtils;

import java.util.UUID;

/**
 * Test context Azure tests.
 */
public class AzureTestContext {
    public static final String CONTAINER_NAME_PREFIX = "pravega-unit-test/";

    public final AzureStorageConfig adapterConfig;

    public final int port;
    public final String configUri;
    public final AzureClient azureClient;
    public final ChunkedSegmentStorageConfig defaultConfig = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;

    public AzureTestContext() throws Exception {
        try {
            this.port = TestUtils.getAvailableListenPort();
            this.configUri = "https://localhost";
            String prefix = CONTAINER_NAME_PREFIX + UUID.randomUUID();
            adapterConfig = getLocalAzureStorageConfig(prefix);
            azureClient = new MockAzureClient(adapterConfig);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    /**
     * Creates a new instance of the AzureStorageConfig class as defined by this builder with the information which includes
     * default configuration to connect Storage account created on Azure portal.
     * @param prefix Prefix of the Pravega owned Azure path under the assigned containers.
     * @return The newly created instance.
     */
    public static AzureStorageConfig getLocalAzureStorageConfig(String prefix) {
        return AzureStorageConfig.builder()
                .with(AzureStorageConfig.ENDPOINT, "http://127.0.0.1:10000/devstoreaccount1")
                .with(AzureStorageConfig.CONNECTION_STRING, 
                      "DefaultEndpointsProtocol=http;"
                    + "AccountName=devstoreaccount1;"
                    + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                    + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
                .with(AzureStorageConfig.CONTAINER, "azureunittests" + System.currentTimeMillis())
                .with(AzureStorageConfig.PREFIX, prefix)
                .with(AzureStorageConfig.CREATE_CONTAINER, false)
                .build();
    }

    public void close() throws Exception {
        if (null != azureClient) {
            azureClient.close();
        }
    }
}
