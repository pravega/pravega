package com.emc.pravega.controller.store;

/**
 * Stream Metadata
 */
public interface StreamMetadataStore {

    boolean addStream(Stream stream);

    Stream getStream(String name);
}
