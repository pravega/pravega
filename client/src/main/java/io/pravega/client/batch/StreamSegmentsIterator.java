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
package io.pravega.client.batch;

import com.google.common.annotations.Beta;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.batch.impl.StreamSegmentsInfoImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.util.Iterator;

@Beta
public interface StreamSegmentsIterator {

    /**
     * This returns an iterator for {@link SegmentRange} specified in {@link BatchClientFactory#getSegments(Stream, StreamCut, StreamCut)}.
     * @return Iterator for {@link SegmentRange}
     */
    Iterator<SegmentRange> getIterator();

    /**
     * This returns the start {@link StreamCut} specified in {@link BatchClientFactory#getSegments(Stream, StreamCut, StreamCut)}.
     * @return Start {@link StreamCut}
     */
    StreamCut getStartStreamCut();

    /**
     * This returns the end {@link StreamCut} specified in {@link BatchClientFactory#getSegments(Stream, StreamCut, StreamCut)}.
     * @return End {@link StreamCut}
     */
    StreamCut getEndStreamCut();

    /**
     * For internal use. Do not call.
     * @return Implementation of StreamSegmentsInfo interface.
     */
    StreamSegmentsInfoImpl asImpl();
}
