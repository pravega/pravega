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
     * Creates a new stream
     * 
     * Note:
     * This method is idempotent assuming called with the same name and config.
     * This method may block.
     * 
     * @param streamName The name of the stream to be created
     * @param config The configuration the stream should use.
     * @return The stream object representing the new stream
     */
    Stream createStream(String streamName, StreamConfiguration config);

    /**
     * Change the configuration for an existing stream.
     * 
     * Note:
     * This method is idempotent assuming called with the same name and config.
     * This method may block.
     * 
     * @param streamName The name of the stream who's config is to be changed
     * @param config The new configuration
     */
    void alterStream(String streamName, StreamConfiguration config);

    /**
     * Returns the requested stream.
     */
    Stream getStream(String streamName);

    @Override
    void close();
}
