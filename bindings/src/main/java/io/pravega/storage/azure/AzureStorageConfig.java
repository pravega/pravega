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
import lombok.Getter;

/**
 * Configuration for the Azure Storage component.
 */
public class AzureStorageConfig {
    public static final Property<String> CONNECTION_STRING = Property.named("connection.string", "", "connectionString");
    public static final Property<String> ENDPOINT = Property.named("endpoint", "", "endpoint");
    public static final Property<String> CONTAINER = Property.named("container", "");
    public static final Property<Boolean> CREATE_CONTAINER = Property.named("container.create", false);
    public static final Property<String> PREFIX = Property.named("prefix", "/");
    private static final String PATH_SEPARATOR = "/";
    private static final String COMPONENT_CODE = "azure";

    /**
     *  Blob service endpoint that we get from the Storage account created on Azure portal.
     */
    @Getter
    private String endpoint;

    /**
     * ConnectionString containing authorization information to access the Storage account.
     */
    @Getter
    private String connectionString;

    /**
     * A unique container name to store set of blobs.
     */
    @Getter
    private String containerName;

    /**
     * Prefix of the Pravega owned Azure path under the assigned container. All the objects under this path will be
     * exclusively owned by Pravega.
     */
    @Getter
    private String prefix;

    /**
     * Whether to create a new container during every run or not.
     */
    @Getter
    private boolean createContainer;

    private AzureStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.endpoint = Preconditions.checkNotNull(properties.get(ENDPOINT));
        this.connectionString = Preconditions.checkNotNull(properties.get(CONNECTION_STRING));
        this.containerName = Preconditions.checkNotNull(properties.get(CONTAINER));
        String givenPrefix = Preconditions.checkNotNull(properties.get(PREFIX), "prefix");
        this.prefix = givenPrefix.endsWith(PATH_SEPARATOR) ? givenPrefix : givenPrefix + PATH_SEPARATOR;
        this.createContainer = properties.getBoolean(CREATE_CONTAINER);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<AzureStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, AzureStorageConfig::new);
    }
}
