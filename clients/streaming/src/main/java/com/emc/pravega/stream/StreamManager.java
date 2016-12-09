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

public interface StreamManager extends AutoCloseable {

    /**
     * Creates a new ConsumerGroup
     * 
     * Note: This method is idempotent assuming called with the same name and config. This method
     * may block.
     * 
     * @param groupName The name of the group to be created.
     * @param streamName The name of the stream the consumer will read from.
     * @param config The configuration for the new ConsumerGroup.
     */
    ConsumerGroup createConsumerGroup(String groupName, String streamName, ConsumerGroupConfig config);
    
    /**
     * Returns the requested consumer group.
     * 
     * @param groupName The name of the group
     * @param streamName The name of the stream the group is associated with.
     */
    ConsumerGroup getConsumerGroup(String groupName, String streamName);
    
    /**
     * Deletes the provided consumer group. No more operations may be performed.
     * Resources used by this group will be freed.
     * 
     * @param group The group to be deleted.
     */
    void deleteConsumerGroup(ConsumerGroup group);
    
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
    void delteStream(Stream toDelete);
}
