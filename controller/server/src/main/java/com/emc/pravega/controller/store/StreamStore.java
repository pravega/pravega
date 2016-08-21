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
package com.emc.pravega.controller.store;

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory stream store.
 */
public class StreamStore implements StreamMetadataStore {

    Map<String, SegmentStore> streams = new HashMap<>();

    public void initialize() {
        streams = new HashMap<>();
        // TODO initialize from persistent store, create collections of appropriate size
    }

    @Override
    public void addStream(String stream) {
        streams.put(stream, new SegmentStore());
    }

    public void addStream(String stream, SegmentStore segments) {
        streams.put(stream, segments);
    }

    @Override
    public SegmentMetadataStore getSegmentMetadata(String stream) {
        return streams.get(stream);
    }

}
