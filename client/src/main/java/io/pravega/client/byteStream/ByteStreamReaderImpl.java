package io.pravega.client.byteStream;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.common.Exceptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.val;

@RequiredArgsConstructor
@ToString
public class ByteStreamReaderImpl extends ByteStreamReader {

    private final SegmentInputStream input;
    private final AtomicBoolean closed = new AtomicBoolean(false); 

    @Override
    public int read(ByteBuffer dst) throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        try {
            ByteBuffer buffer = input.read(dst.remaining());
            int bytesRead = buffer.remaining();
            buffer.get
            buffer.get(b, 0, bytesRead);
            return bytesRead;
        } catch (EndOfSegmentException e) {
            return -1;
        }
    }

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public long getOffset() {
        return input.getOffset();
    }

    @Override
    public void jumpToOffset(long offset) throws InvalidOffsetException {
        Exceptions.checkNotClosed(closed.get(), this);
        input.setOffset(offset);
    }

    @Override
    public int available() {
        Exceptions.checkNotClosed(closed.get(), this);
        //Can't replace isSegmentReady because does not handle EOF correctly. Need to have a way to deal with that.
        return input.bytesInBuffer();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            input.close();
        }
    }

    @Override
    public long fetchTailOffset() {
        // TODO Auto-generated method stub
    }

    @Override
    public int read() throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        try {
            ByteBuffer read = input.read(1);
            return read.get() & 0xFF;
        } catch (EndOfSegmentException e) {
            return -1;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        try {
            ByteBuffer buffer = input.read(b.length);
            int bytesRead = buffer.remaining();
            buffer.get(b, 0, bytesRead);
            return bytesRead;
        } catch (EndOfSegmentException e) {
            return -1;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        try {
            ByteBuffer buffer = input.read(b.length);
            int bytesRead = buffer.remaining();
            buffer.get(b, off, bytesRead);
            return bytesRead;
        } catch (EndOfSegmentException e) {
            return -1;
        }
    }

    @Override
    public long skip(long n) {
        Exceptions.checkNotClosed(closed.get(), this);
        //TODO: Threadsafety...
        long offset = input.getOffset();
        long endOffset = fetchTailOffset();
        long newOffset = Math.min(offset+skip, endOffset);
        input.setOffset(newOffset);
        return newOffset - offset;
    }

    @Override
    public CompletableFuture<Void> onDataAvailable() {
        Exceptions.checkNotClosed(closed.get(), this);
        return input.fillBuffer().thenApply(v -> available());
    }

}
