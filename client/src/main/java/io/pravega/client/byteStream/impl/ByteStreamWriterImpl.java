package io.pravega.client.byteStream.impl;

import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.impl.PendingEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ByteStreamWriterImpl extends ByteStreamWriter {
    
    private final SegmentOutputStream out;
    private final SegmentMetadataClient meta;

    @Override
    public void write(ByteBuffer src) throws IOException {
        out.write(PendingEvent.withoutHeader(null, src, null));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer data = ByteBuffer.wrap(b, off, len);
        out.write(PendingEvent.withoutHeader(null, data, null));
    }

    @Override
    public void close() throws IOException {
        try {
            out.close();
        } catch (SegmentSealedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
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
        return meta.fetchCurrentSegmentLength();
    }

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }

}
