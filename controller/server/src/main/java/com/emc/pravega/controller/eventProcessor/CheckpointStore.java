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
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.stream.Position;

import java.util.Map;

/**
 * Store to map
 * 1. Process to (readerGroup, reader), and
 * 2. Readers to their last position
 */
public interface CheckpointStore {

    enum StoreType {
        InMemory,
        Zookeeper,
        StateSynchronizer
    }

    void setPosition(final String host, final String readerGroup, final String readerId, final Position position);

    Map<String, Position> getPositions(final String host, final String readerGroup);
}
