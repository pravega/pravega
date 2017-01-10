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

import java.util.List;

/**
 * Interface for listener for changes to stream.
 */
public interface StreamChangeListener {
    /**
     * Callback for when a new stream is added to the system.
     * @param stream                Stream details.
     */
    void addStream(final StreamData stream);

    /**
     * Callback for when a stream is removed from the cluster.
     * @param stream                Stream name.
     * @param scope                 Stream scope.
     */
    void removeStream(final String stream, final String scope);

    /**
     * Callback for when stream's configuration is altered.
     * @param stream                Stream name.
     * @param scope                 Stream scope.
     * @param streamConfiguration   New stream configuration.
     */
    void updateStream(final String stream, final String scope, final StreamConfiguration streamConfiguration);

    /**
     * Callback for when a stream's scaled operation completes.
     * @param stream                Stream name.
     * @param scope                 Stream scope.
     * @param activeSegments        New list of active segments after the scaled operation completes.
     */
    void scaledStream(final String stream, final String scope, final List<Segment> activeSegments);
}
