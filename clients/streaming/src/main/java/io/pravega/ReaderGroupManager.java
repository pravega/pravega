/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega;

import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroup;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.Serializer;
import io.pravega.stream.impl.ReaderGroupManagerImpl;

import java.net.URI;
import java.util.Set;

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
    ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, Set<String> streamNames);
    
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
