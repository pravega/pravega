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
import java.util.Iterator;

/**
 * Please note this is an experimental API.
 * 
 * Allows for reading data from a segment. Returns an item from {@link #next()} for each event in
 * the segment at the time of its creation. Once all the events that were in the segment at the time
 * of the creation of the SegmentIterator have been returned {@link #hasNext()} will return false.
 *
 * While buffering is used to avoid it, it is possible for {@link #next()} to block on fetching the
 * data.
 *
 * While iterating over SegmentIterator using {@link #next()} it can throw {@link io.pravega.client.stream.TruncatedDataException}
 * if SegmentIterator is pointing to an offset which is already truncated.
 * If this exception occurs, SegmentIterator will automatically update headOffset to next available offset.
 * Next call to {@link #next()} will point correctly to valid offset and reader will continue.
 * 
 * At any time {@link #getOffset()} can be called to get the byte offset in the segment the iterator
 * is currently pointing to.
 * 
 * @param <T> The type of the events written to this segment.
 */
@Beta
public interface SegmentIterator<T> extends Iterator<T>, AutoCloseable {

    /**
     * Provides the current offset in the segment.
     * 
     * @return The current offset in the segment
     */
    long getOffset();

    /**
     * Closes the iterator, freeing any resources associated with it.
     * 
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

}
