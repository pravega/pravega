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
package io.pravega.client.byteStream.impl;

import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.stream.impl.PendingEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ByteStreamWriterImpl extends ByteStreamWriter {
    
    @NonNull
    private final SegmentOutputStream out;
    @NonNull
    private final SegmentMetadataClient meta;

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }
    
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
        out.close();
        meta.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void closeAndSeal() throws IOException {
        out.close();
        meta.sealSegment();
        meta.close();
    }

    @Override
    public long fetchTailOffset() {
        return meta.fetchCurrentSegmentLength();
    }

    @Override
    public void truncateDataBefore(long offset) {
        meta.truncateSegment(offset);
    }

}
