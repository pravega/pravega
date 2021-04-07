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
package io.pravega.client.stream;

import java.io.Serializable;

import lombok.Builder;
import lombok.Data;

/**
 * The configuration of a Stream.
 */
@Data
@Builder
public class StreamConfiguration implements Serializable {
    
    private static final long serialVersionUID = 1L;

    /**
     * API to return scaling policy.
     *
     * @param scalingPolicy The Stream Scaling policy.
     * @return Scaling Policy for the Stream.
     */
    private final ScalingPolicy scalingPolicy;

    /**
     * API to return retention policy.
     * Also see: {@link ReaderGroupConfig.StreamDataRetention}
     * @param retentionPolicy The Stream Retention policy.
     * @return Retention Policy for the Stream.
     */
    private final RetentionPolicy retentionPolicy;
    
    /**
     * The duration after the last call to {@link EventStreamWriter#noteTime(long)} which the
     * timestamp should be considered valid before it is forgotten. Meaning that after this long of
     * not calling {@link EventStreamWriter#noteTime(long)} the writer will be forgotten.
     * If there are no known writers, readers that call {@link EventStreamReader#getCurrentTimeWindow(Stream)}
     * will receive a `null` when they are at the corresponding position in the stream.
     *
     * @param timestampAggregationTimeout The duration after the last call to {@link EventStreamWriter#noteTime(long)}
     *                                    which the timestamp should be considered valid before it is forgotten.
     * @return The duration after the last call to {@link EventStreamWriter#noteTime(long)} which the timestamp should
     * be considered valid before it is forgotten.
     */
    private final long timestampAggregationTimeout;

    public static final class StreamConfigurationBuilder {
        private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    }
}
