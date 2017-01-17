/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega;

import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamManagerImpl;

import java.net.URI;
import java.util.List;

/**
 * Used to create, delete, and manage Streams and ReaderGroups.
 */
public interface StreamManager extends AutoCloseable {

    public static StreamManager withScope(String scope, URI controllerUri) {
        return new StreamManagerImpl(scope, controllerUri);
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
     */
    ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, List<String> streamNames);
    
    /**
     * Updates a reader group. The reader group will have a new {@link ReaderGroup#getRevision()}
     * 
     * All existing readers will have to call
     * {@link ClientFactory#createReader(String, String, Serializer, ReaderConfig)}
     * . If they continue to read events they will eventually encounter an error.
     * 
     * Readers connecting to the group will start from the point defined in the config, exactly as
     * though it were a new reader group.
     * 
     * @param groupName The name of the group to be created.
     * @param config The configuration for the new ReaderGroup.
     * @param streamNames The name of the streams the reader will read from.
     */
    ReaderGroup updateReaderGroup(String groupName, ReaderGroupConfig config, List<String> streamNames);
    
    /**
     * Returns the requested reader group.
     * 
     * @param groupName The name of the group
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
     * Creates a new stream
     * <p>
     * Note: This method is idempotent assuming called with the same name and config. This method
     * may block.
     *
     * @param streamName The name of the stream to be created.
     * @param config The configuration the stream should use.
     * @return The stream object representing the new stream.
     */
    Stream createStream(String streamName, StreamConfiguration config);

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
     * Returns the requested stream.
     *
     * @param streamName The name of the stream to get.
     */
    Stream getStream(String streamName);
    
    /**
     * Deletes the provided stream. No more events may be written or read.
     * Resources used by the stream will be freed.
     * @param toDelete The stream to be deleted.
     */
    void deleteStream(Stream toDelete);
}
