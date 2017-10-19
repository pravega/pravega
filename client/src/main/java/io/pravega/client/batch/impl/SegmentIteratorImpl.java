package io.pravega.client.batch.impl;

import com.google.common.annotations.Beta;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.stream.Serializer;
import java.util.NoSuchElementException;
import lombok.Getter;
import lombok.SneakyThrows;

@Beta
public class SegmentIteratorImpl<T> implements SegmentIterator<T> {

    private final Serializer<T> deserializer;
    @Getter
    private final long startingOffset;
    private final long endingOffset;
    private final SegmentInputStream input;

    public SegmentIteratorImpl(SegmentInputStreamFactory factory, Segment segment,
            Serializer<T> deserializer, long startingOffset, long endingOffset) {
        this.deserializer = deserializer;
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        input = factory.createInputStreamForSegment(segment);
        input.setOffset(startingOffset);        
    }

    @Override
    public boolean hasNext() {
        return input.getOffset() < endingOffset;
    }

    @Override
    @SneakyThrows(EndOfSegmentException.class) //endingOffset should make this impossible.
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        } 
        return deserializer.deserialize(input.read());
    }

    @Override
    public long getOffset() {
        return input.getOffset();
    }

    @Override
    public void close() {
        input.close();
    }

}
