package io.pravega.client.byteStream.impl;

import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventStreamWriter;
import java.nio.ByteBuffer;

public interface BufferedSegmentInputStream extends AutoCloseable {
        
        Segment getSegmentId();
        
        /**
         * Sets the offset for reading from the segment.
         *
         * @param offset The offset to set.
         */
        public abstract void setOffset(long offset);

        /**
         * Gets the current offset. (Passing this to setOffset in the future will reset reads to the
         * current position in the segment.)
         *
         * @return The current offset.
         */
        public abstract long getOffset();

        /**
         * Reads bytes from the segment a single event. Buffering is performed internally to try to prevent
         * blocking. If there is no event after timeout null will be returned. EndOfSegmentException indicates the
         * segment has ended an no more events may be read.
         *
         * @return A ByteBuffer containing the serialized data that was written via
         *         {@link EventStreamWriter#writeEvent(String, Object)}
         * @throws EndOfSegmentException If no event could be read because the end of the segment was reached.
         * @throws SegmentTruncatedException If the segment has been truncated beyond the current offset and the data cannot be read.
         */
        public abstract ByteBuffer read(int numBytes) throws EndOfSegmentException, SegmentTruncatedException;
        
        /**
         * Issue a request to asynchronously fill the buffer. To hopefully prevent future {@link #read()} calls from blocking.
         * Calling this multiple times is harmless.
         */
        public abstract void fillBuffer();
        
        /**
         * Closes this InputStream. No further methods may be called after close.
         * This will free any resources associated with the InputStream.
         */
        @Override
        public abstract void close();
        
        public int bytesInBuffer();

}
