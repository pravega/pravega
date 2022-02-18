package io.pravega.client.stream.mock;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.concurrent.Futures;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class MockByteStreamWriter extends ByteStreamWriter {

    @NonNull
    private final SegmentOutputStream out;
    @NonNull
    private final SegmentMetadataClient meta;
    private CompletableFuture<Void> latestEventFuture;

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(ByteBuffer src) throws IOException {
        out.write(PendingEvent.withoutHeader(null, src, updateLastEventFuture()));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer data = ByteBuffer.wrap(b, off, len);
        out.write(PendingEvent.withoutHeader(null, data, updateLastEventFuture()));
    }

    @VisibleForTesting
    @Synchronized
    public CompletableFuture<Void> updateLastEventFuture() {
        this.latestEventFuture = new CompletableFuture<>();
        return latestEventFuture;
    }

    @Override
    public void close() throws IOException {
        out.close();
        meta.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> flushAsync() {
        return this.latestEventFuture;
    }

    @Override
    public void closeAndSeal() throws IOException {
        out.close();
        Futures.getThrowingException(meta.sealSegment());
        meta.close();
    }

    @Override
    public long fetchHeadOffset() {
        return Futures.getThrowingException(meta.fetchCurrentSegmentHeadOffset());
    }

    @Override
    public long fetchTailOffset() {
        return Futures.getThrowingException(meta.fetchCurrentSegmentLength());
    }

    @Override
    public void truncateDataBefore(long offset) {
        Futures.getThrowingException(meta.truncateSegment(offset));
    }
}
