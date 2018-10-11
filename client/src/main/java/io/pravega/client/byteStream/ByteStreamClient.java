package io.pravega.client.byteStream;

import com.google.common.annotations.Beta;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventWriterConfig;

public interface ByteStreamClient {
    @Beta
    ByteStreamReader createByteStreamReaders(Segment segment);
    
    @Beta
    ByteStreamWriter createByteStreamWriter(Segment segment, EventWriterConfig config);
}
