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

import com.google.common.base.Preconditions;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.storage.s3.S3StorageConfig;
import lombok.Builder;
import lombok.Getter;

@Builder
public class AzureStorageConfig {

    public static final Property<String> CONNECTION_STRING = Property.named("connection.string", "", "connectionString");
    public static final Property<String> CONTAINER = Property.named("container", "");
    public static final Property<String> ACCESS_KEY = Property.named("connect.config.access.key", "");
    public static final Property<String> PREFIX = Property.named("prefix", "/");
    private static final String PATH_SEPARATOR = "/";
    private static final String COMPONENT_CODE = "azure";

    @Getter
//    private final String container;
    private String endpoint = "https://ajadhav9.blob.core.windows.net";
    @Getter
    private String connectionString = "DefaultEndpointsProtocol=https;AccountName=ajadhav9;AccountKey=0DuaCG/7yEpHQCE7lS/hkxHtQa1oqg2E7NSXSLCPGjTvBrGHDdn8zxiYaA1iPn84ntErNXX0AMYB+AStK7xMCA==;EndpointSuffix=core.windows.net";
    @Getter
    private String containerName;
    @Getter
    private String prefix = "test";

    private AzureStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.connectionString = Preconditions.checkNotNull(properties.get(CONNECTION_STRING));
        this.containerName = Preconditions.checkNotNull(properties.get(CONTAINER));
        String givenPrefix = Preconditions.checkNotNull(properties.get(PREFIX), "prefix");
        this.prefix = givenPrefix.endsWith(PATH_SEPARATOR) ? givenPrefix : givenPrefix + PATH_SEPARATOR;
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<AzureStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, AzureStorageConfig::new);
    }
}
