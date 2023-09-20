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
package io.pravega.client;

import com.google.common.annotations.Beta;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.batch.impl.BatchClientFactoryImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.util.List;
import lombok.val;

/**
 * Please note this is an experimental API.
 * 
 * Used to get metadata about and read from an existing streams.
 * 
 * All events written to a stream will be visible to SegmentIterators
 * 
 * Events within a segment are strictly ordered, but as this API allows for reading from multiple
 * segments in parallel without adhering to time ordering. This allows for even greater
 * parallelization at the expense of the ordering guarantees provided by {@link EventStreamReader}.
 */
@Beta
public interface BatchClientFactory extends AutoCloseable {

    /**
     * Creates a new instance of BatchClientFactory.
     *
     * @param scope The scope of the stream.
     * @param config Configuration for the client.
     * @return Instance of BatchClientFactory implementation.
     */
    static BatchClientFactory withScope(String scope, ClientConfig config) {
        val connectionFactory = new SocketConnectionFactoryImpl(config);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                           connectionFactory.getInternalExecutor());
        return new BatchClientFactoryImpl(controller, config, connectionFactory);
    }
    
    /**
     * Provides a list of segments for a given stream between fromStreamCut and toStreamCut.
     * Passing StreamCut.UNBOUNDED or null to fromStreamCut and toStreamCut will result in using the current start of
     * stream and the current end of stream respectively.
     *
     * Note: In case of stream truncation: <p>
     * - Passing a null to fromStreamCut will result in using the current start of the Stream post truncation.<p>
     * - Passing a fromStreamCut which points to the truncated stream will result in a {@link NoSuchSegmentException} while
     * iterating over SegmentRange iterator obtained via {@link StreamSegmentsIterator#getIterator()}
     *
     * @param stream the stream.
     * @param fromStreamCut starting stream cut.
     * @param toStreamCut end stream cut.
     * @return Segment information between the two stream cuts.
     */
    StreamSegmentsIterator getSegments(Stream stream, StreamCut fromStreamCut, StreamCut toStreamCut);

    /**
     * Provides a SegmentIterator to read the events in the requested segment starting from the
     * beginning of the segment and ending at the current end of the segment.
     * 
     * @param <T> The type of events written to the segment.
     * @param segment The segment to read from
     * @param deserializer A deserializer to be used to parse events
     * @return A SegmentIterator over the requested segment
     */
    <T> SegmentIterator<T> readSegment(SegmentRange segment, Serializer<T> deserializer);

    /**
     * Closes the client factory. This will close any connections created through it.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

    /**
     * Provides a list of SegmentRange in between a start and end streamCut.
     *
     * @param startStreamCut start streamCut.
     * @param endStreamCut end streamCut.
     * @return A list of segment range in between a start and end stream cut.
     */
    List<SegmentRange> getSegmentRangeBetweenStreamCuts(final StreamCut startStreamCut, final StreamCut endStreamCut);

    /**
     * Provides a streamcut approximately the requested distance after the starting streamcut.
     * The returned stream cut will not 'skip over' scaling events. So all the segments in the returned 
     * stream cut will either be in the starting stream cut or be immediate successors to those segments.
     * (This is so that if this method is called in a loop and all the data between the starting and 
     * returned stream cut is read before next invocation, the data will be read in order)
     * 
     * @param startingStreamCut Starting streamcut
     * @param approxDistanceToNextOffset approx distance to nextoffset in bytes
     * @return A streamcut after the apporoximate distance from the startingStreamCut.
     * @throws SegmentTruncatedException If the data at the starting streamcut has been truncated
     *             away and can no longer be read. (In such a case it may be best to restart reading
     *             from {@link StreamManager#fetchStreamInfo(String, String)}'s {@link StreamInfo#getHeadStreamCut()}
     */
    StreamCut getNextStreamCut(final StreamCut startingStreamCut, long approxDistanceToNextOffset) throws SegmentTruncatedException;
}
