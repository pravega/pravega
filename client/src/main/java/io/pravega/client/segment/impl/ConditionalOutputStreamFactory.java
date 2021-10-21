/**
 * Copyright Pravega Authors.
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
package io.pravega.client.segment.impl;

import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.stream.EventWriterConfig;

/**
 * Creates {@link ConditionalOutputStream} for conditional appends on existing segments.
 */
public interface ConditionalOutputStreamFactory {
    /**
     * Opens an existing segment for conditional append operations. This operation will throw
     * {@link NoSuchSegmentException} if the segment does not exist. This operation may be called
     * multiple times on the same segment from the same client (i.e., there can be concurrent
     * conditional clients in the same process space).
     *
     * @param segment The segment to create a conditional client for.
     * @param tokenProvider The {@link DelegationTokenProvider} instance to be used for obtaining a delegation token.
     * @param config output writer configuration.
     * @return New instance of ConditionalOutputStream for the provided segment.
     */
    ConditionalOutputStream createConditionalOutputStream(Segment segment, DelegationTokenProvider tokenProvider, EventWriterConfig config);

}
