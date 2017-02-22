/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega;

import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamManagerImpl;

import java.net.URI;

/**
 * Used to create, delete, and manage Streams and ReaderGroups.
 */
public interface StreamManager extends AutoCloseable {

    /**
     * Creates a new instance of StreamManager.
     *
     * @param scope The Scope string.
     * @param controllerUri The Controller URI.
     * @return Instance of Stream Manager implementation.
     */
    public static StreamManager withScope(String scope, URI controllerUri) {
        return new StreamManagerImpl(scope, controllerUri);
    }
  
    /**
     * Creates a new stream
     * <p>
     * Note: This method is idempotent assuming called with the same name and config. This method
     * may block.
     *
     * @param streamName The name of the stream to be created.
     * @param config The configuration the stream should use.
     */
    void createStream(String streamName, StreamConfiguration config);

    /**
     * Change the configuration for an existing stream.
     * <p>
     * Note:
     * This method is idempotent assuming called with the same name and config.
     * This method may block.
     *
     * @param streamName The name of the stream who's config is to be changed.
     * @param config     The new configuration.
     */
    void alterStream(String streamName, StreamConfiguration config);

    /**
     * Seal an existing stream.
     *
     * @param streamName The name of the stream which has to be sealed.
     */
    void sealStream(String streamName);
    
    /**
     * Deletes the provided stream. No more events may be written or read.
     * Resources used by the stream will be freed.
     * @param toDelete The name of the stream to be deleted.
     */
    void deleteStream(String toDelete);
    
    /**
     * See @see java.lang.AutoCloseable#close() .
     */
    @Override
    void close();
}
