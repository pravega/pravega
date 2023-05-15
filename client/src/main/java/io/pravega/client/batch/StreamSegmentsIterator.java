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
import java.util.List;

/**
 * @author tom
 *
 */
@Beta
public interface StreamSegmentsIterator extends Iterator<List<SegmentRange>> {

    /**
     * This returns the start {@link StreamCut} specified in {@link BatchClientFactory#getSegments(Stream, StreamCut, StreamCut)}.
     * Note: If {@link StreamCut.UNBOUNDED} was specified this method will instead 
     * return the start of the stream at the time this StreamSegmentsIterator was created.
     * 
     * @return Start {@link StreamCut}
     */
    StreamCut getStartStreamCut();

    /**
     * This returns the end {@link StreamCut} specified in {@link BatchClientFactory#getSegments(Stream, StreamCut, StreamCut)}.
     * Note: If {@link StreamCut.UNBOUNDED} was specified this method will instead 
     * return the end of the stream at the time this StreamSegmentsIterator was created.
     * 
     * @return End {@link StreamCut}
     */
    StreamCut getEndStreamCut();
    
    /**
     * This returns the {@link StreamCut} which has been iterated up to.
     * This will start as the same value as {@link StreamSegmentsIterator#getStartStreamCut()} 
     * and increase every time {@link #next()} is called until it reaches {@link #getEndStreamCut()}
     * 
     * @return End {@link StreamCut}
     */
    StreamCut getCurrentStreamCut();
    
    @Override
    boolean hasNext();
    
    /**
     * Get the next set of SegmentRanges
     * 
     * @return the next set of SegmentRanges between {@link #getStartStreamCut()} and {@link #getEndStreamCut()}
     * (In order) If the endStreamCut has been reached, returns null.
     */
    List<SegmentRange> next();

    /**
     * Get the next set of SegmentRanges where each is contains covers approximately the requested number of bytes.
     * 
     * @param size The size in bytes each of the returned segment ranges should be limited to.
     * 
     * @return the next set of SegmentRanges between {@link #getStartStreamCut()} and {@link #getEndStreamCut()}
     * (In order) If the endStreamCut has been reached, returns null.
     */
    List<SegmentRange> next(long size);
    
    /**
     * For internal use. Do not call.
     * @return Implementation of StreamSegmentsInfo interface.
     */
    StreamSegmentsInfoImpl asImpl();
    
    /**
     * @deprecated use {@link #next()} directly
     * This returns an iterator for {@link SegmentRange} specified in {@link BatchClientFactory#getSegments(Stream, StreamCut, StreamCut)}.
     * @return Iterator for {@link SegmentRange}
     */
    @Deprecated
    Iterator<SegmentRange> getIterator();
}
