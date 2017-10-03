package io.pravega.controller.store.stream;

import io.pravega.client.stream.StreamConfiguration;
import lombok.Data;

import java.io.Serializable;

@Data
public class StreamConfigWithVersion implements Serializable {

    private final StreamConfiguration configuration;

    private final int version;

    public static StreamConfigWithVersion generateNext(final StreamConfigWithVersion previous,
                                                       final StreamConfiguration newConfig) {
        return new StreamConfigWithVersion(newConfig, previous.getVersion() + 1);
    }
}