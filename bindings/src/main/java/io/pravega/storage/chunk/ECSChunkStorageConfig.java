package io.pravega.storage.chunk;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ECSChunkStorageConfig {


    public static final Property<String> CONFIGURI = Property.named("connect.config.uri", "", "configUri");
    public static final Property<String> BUCKET = Property.named("bucket", "chunk-obj");
    public static final Property<String> PREFIX = Property.named("prefix", "pravega-tier2");

    private static final String COMPONENT_CODE = "ecschunk";
    private static final String PATH_SEPARATOR = "/";
    private static final String URI_SEPARATOR = ";";
    @Getter
    private final String bucket;

    @Getter
    private final List<URI> endpoints;

    @Getter
    private final String prefix;

    @Getter
    private final int chunkSize = 16 * 1024 * 1024;
    @Getter
    public final int indexGranularity = 131072;

    @Getter
    public final String HeaderEMCExtensionIndexGranularity = "x-emc-index-granularity";
    /**
     * Creates a new instance of the ECSChunkStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ECSChunkStorageConfig(TypedProperties properties) throws ConfigurationException {
        String[] configUris = Preconditions.checkNotNull(properties.get(CONFIGURI), "configUri").split(URI_SEPARATOR);
        this.endpoints = new ArrayList<>();
        for (String uri : configUris) {
            endpoints.add(URI.create(uri));
        }
        this.bucket = Preconditions.checkNotNull(properties.get(BUCKET), "bucket");
        String givenPrefix = Preconditions.checkNotNull(properties.get(PREFIX), "prefix");
        this.prefix = givenPrefix.endsWith(PATH_SEPARATOR) ? givenPrefix : givenPrefix + PATH_SEPARATOR;
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ECSChunkStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ECSChunkStorageConfig::new);
    }
}
