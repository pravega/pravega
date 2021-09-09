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
package io.pravega.client.admin;

import com.google.common.annotations.Beta;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import lombok.Data;

/**
 * This class is used to represent Stream information. It currently includes:
 *
 *  <ul>
 *  <li> Scope of stream. </li>
 *  <li> Name of stream. </li>
 *  <li> {@link StreamCut} which represents the current TAIL of the stream. </li>
 *  <li> {@link StreamCut} which represents the current HEAD of the stream. </li>
 *  <li> Flag which is set to True if the stream is Sealed. </li>
 *  </ul>
 */
@Beta
@Data
public class StreamInfo {
    /**
     * Scope name of the stream.
     *
     * @param scope Scope name of the stream.
     * @return Scope name of the stream.
     */
    private final String scope;

    /**
     * Stream name.
     *
     * @param streamName Stream name.
     * @return Stream name.
     */
    private final String streamName;

    /**
     * Stream Configuration.
     * This includes the {@link io.pravega.client.stream.ScalingPolicy}, {@link io.pravega.client.stream.RetentionPolicy}
     * and the tags associated with the stream.
     *
     * @param streamConfiguration Stream configuration.
     * @return Stream configuration.
     */
    private final StreamConfiguration streamConfiguration;

    /**
     * {@link StreamCut} representing the current TAIL of the stream.
     *
     * @param tailStreamCut {@link StreamCut} representing the current TAIL of the stream.
     * @return {@link StreamCut} representing the current TAIL of the stream.
     */
    private final StreamCut tailStreamCut;

    /**
     * {@link StreamCut} representing the current HEAD of the stream.
     *
     * @param headStreamCut {@link StreamCut} representing the current HEAD of the stream.
     * @return {@link StreamCut} representing the current HEAD of the stream.
     */
    private final StreamCut headStreamCut;

    /**
     * Indicates whether the Stream is sealed (true) or not (false). If a stream is sealed, then no further Events
     * can be written to it.
     *
     * @param sealed Indicates whether the Stream is sealed (true) or not (false).
     * @return Indicates whether the Stream is sealed (true) or not (false).
     */
    private final boolean sealed;
}
