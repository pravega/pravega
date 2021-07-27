package io.pravega.storage.chunk;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

import java.net.URI;

public class ECSChunkStorageConfig {

    @Getter
    private static final String bucket = "chunk-obj";

    @Getter
    private static final URI endpoint = URI.create("http://127.0.0.1:9939");

    @Getter
    private static final int chunkSize = 16 * 1024 * 1024;
    @Getter
    public static final int indexGranularity = 131072;

    @Getter
    public static final String HeaderEMCExtensionIndexGranularity = "x-emc-index-granularity";
    /**
     * Creates a new instance of the ECSChunkStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ECSChunkStorageConfig(TypedProperties properties) throws ConfigurationException {

    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ECSChunkStorageConfig> builder() {
        return new ConfigBuilder<>("COMPONENT_CODE", ECSChunkStorageConfig::new);
    }
}
