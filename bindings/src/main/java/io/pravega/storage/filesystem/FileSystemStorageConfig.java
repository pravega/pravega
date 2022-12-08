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
package io.pravega.storage.filesystem;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the NFS Storage component.
 */
@Slf4j
public class FileSystemStorageConfig {
    //region Config Names

    public static final Property<String> ROOT = Property.named("root", "/fs/");
    public static final Property<Boolean> REPLACE_ENABLED = Property.named("replace.enable", false);
    public static final String COMPONENT_CODE = "filesystem";

    //endregion

    //region Members

    /**
     * Root of the Pravega owned filesystem path. All the directories/files under this path will be exclusively
     * owned by Pravega.
     */
    @Getter
    private final String root;

    /**
     * Whether the {@link FileSystemChunkStorage} instance supports replacement or not.
     */
    @Getter
    private final boolean replaceEnabled;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileSystemStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private FileSystemStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.root = properties.get(ROOT);
        this.replaceEnabled = properties.getBoolean(REPLACE_ENABLED);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<FileSystemStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, FileSystemStorageConfig::new);
    }

    //endregion
}
