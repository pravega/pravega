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
package com.emc.pravega.stream;

import com.emc.pravega.ClientFactory;

import java.util.List;
import java.util.Set;

/**
 * A reader group is a collection of readers that collectively read all the events in the
 * stream. The events are distributed among the readers in the group such that each event goes
 * to only one reader.
 * 
 * The readers in the group may change over time. Readers are added to the group by calling
 * {@link ClientFactory#createReader(String, String, Serializer, ReaderConfig)} and are removed by
 * calling {@link #readerOffline(String, Position)}
 */
public interface ReaderGroup {

    /**
     * Returns the scope of the stream which the group is associated with.
     * @return a scope string
     */
    String getScope();

    /**
     * Returns the names of the streams the group is associated with.
     * @return list of stream names
     */
    List<String> getStreamNames();

    /**
     * Returns the name of the group.
     * @return reader group name
     */
    String getGroupName();

    /**
     * Returns the configuration of the reader group.
     * @return reader group configuration
     */
    ReaderGroupConfig getConfig();

    /**
     * Invoked when a reader that was added to the group is no longer consuming events. This will
     * cause the events that were going to that reader to be redistributed among the other
     * readers. Events after the lastPosition provided will be (re)read by other readers in the
     * {@link ReaderGroup}.
     * 
     * Note that this method is automatically invoked by {@link EventStreamReader#close()}
     * 
     * @param readerId The id of the reader that is offline.
     * @param lastPosition The position of the last event that was successfully processed by the
     *        reader.
     */
    void readerOffline(String readerId, Position lastPosition);
    
    /**
     * Returns a set of readerIds for the readers that are considered to be online by the group.
     * i.e. {@link ClientFactory#createReader(String, String, Serializer, ReaderConfig)} was called but
     * {@link #readerOffline(String, Position)} was not called subsequently.
     * @return set of active reader IDs of the group
     */
    Set<String> getOnlineReaders();
}
