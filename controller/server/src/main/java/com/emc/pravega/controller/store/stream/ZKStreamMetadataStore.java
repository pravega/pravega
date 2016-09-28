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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.stream.StreamConfiguration;

/**
 * ZK stream metadata store
 */
public class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    @Override
    public ZKStream getStream(String name) {
        // Return a new stream object each time.
        // This object may not have real metadata wrapped. It may pull it on a need basis from ZK store.
        // This object should not be cached in this store, for the following reasons.
        // (1) controller is stateless,
        // (2) other controller instances may modify stream metadata, thus rendering this cache stale, specifically,
        // get currently active segments may return stale value.
        return new ZKStream(name);
    }

    @Override
    public boolean createStream(String name, StreamConfiguration configuration) {
        // We do not cache the created stream
        return new ZKStream(name).create(configuration);
    }
}
