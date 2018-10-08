package io.pravega.client.byteStream.impl;

import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.util.CircularBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ByteStreamWriterImpl extends ByteStreamWriter {
    
    private final SegmentOutputStream out;
    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    private final CircularBuffer buffer = new CircularBuffer(4096);

    @Override
    public void write(ByteBuffer src) throws IOException {
        out.write(new PendingEvent(routingKey, data, ackFuture));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer data = ByteBuffer.wrap(b, off, len);
        out.write(event);
    }

    @Override
    public void setThrowBeforeBlocking(boolean shouldThrow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        out.close();
    }

    @Override
    public void flush() throws IOException {
        out.close();
    }

    @Override
    public void closeAndSeal() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long fetchPersistedOffset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }

}
