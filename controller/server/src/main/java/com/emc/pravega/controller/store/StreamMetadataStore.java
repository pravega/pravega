package com.emc.pravega.controller.store;

/**
 * Stream Metadata
 */
public interface StreamMetadataStore {

    void addStream(String stream);

    SegmentMetadataStore getSegmentMetadata(String stream);
}
