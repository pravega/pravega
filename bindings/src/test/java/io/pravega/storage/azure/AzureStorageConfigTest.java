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

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link AzureStorageConfig}
 */
public class AzureStorageConfigTest {

    @Test
    public void testDefaultAzureConfig() {
        ConfigBuilder<AzureStorageConfig> builder = AzureStorageConfig.builder();
        builder.with(Property.named("endpoint"), "http://127.0.0.1:10000/devstoreaccount1")
               .with(Property.named("connection.string"),
                     "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
                   + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                   + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
                .with(Property.named("container"), "testContainer")
                .with(Property.named("prefix"), "testPrefix");
        AzureStorageConfig config = builder.build();
        assertEquals("testContainer", config.getContainerName());
        assertEquals("testPrefix/", config.getPrefix());
        assertEquals(false, config.isCreateContainer());
    }

    @Test
    public void testConstructAzureConfig() {
        ConfigBuilder<AzureStorageConfig> builder = AzureStorageConfig.builder();
        builder.with(Property.named("endpoint"), "http://example.com")
                .with(Property.named("connection.string"), "connection.string")
                .with(Property.named("container"), "testContainer")
                .with(Property.named("prefix"), "testPrefix")
                .with(Property.named("container.create"), true);
        AzureStorageConfig config = builder.build();
        assertEquals("http://example.com", config.getEndpoint());
        assertEquals("connection.string", config.getConnectionString());
        assertEquals("testContainer", config.getContainerName());
        assertEquals("testPrefix/", config.getPrefix());
        assertEquals( true, config.isCreateContainer());
    }
}
