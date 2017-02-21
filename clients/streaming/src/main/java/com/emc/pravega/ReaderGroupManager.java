/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega;

import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ReaderGroupManagerImpl;

import java.net.URI;
import java.util.List;

public interface ReaderGroupManager extends AutoCloseable {
    
    /**
     * Creates a new instance of StreamManager.
     *
     * @param scope The Scope string.
     * @param controllerUri The Controller URI.
     * @return Instance of Stream Manager implementation.
     */
    public static ReaderGroupManager withScope(String scope, URI controllerUri) {
        return new ReaderGroupManagerImpl(scope, controllerUri);
    }
    
    /**
     * Creates a new ReaderGroup
     * 
     * Readers will be able to join the group by calling
     * {@link ClientFactory#createReader(String, String, Serializer, ReaderConfig)}
     * . Once this is done they will start receiving events from the point defined in the config
     * passed here.
     * 
     * Note: This method is idempotent assuming called with the same name and config. This method
     * may block.
     * 
     * @param groupName The name of the group to be created.
     * @param config The configuration for the new ReaderGroup.
     * @param streamNames The name of the streams the reader will read from.
     * @return Newly created ReaderGroup object
     */
    ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, List<String> streamNames);
    
    /**
     * Returns the requested reader group.
     * 
     * @param groupName The name of the group
     * @return Reader group with the given name
     */
    ReaderGroup getReaderGroup(String groupName);
    
    /**
     * Deletes the provided reader group. No more operations may be performed.
     * Resources used by this group will be freed.
     * 
     * @param group The group to be deleted.
     */
    void deleteReaderGroup(ReaderGroup group);
    
    /**
     * See @see java.lang.AutoCloseable#close() .
     */
    @Override
    void close();
    
}
