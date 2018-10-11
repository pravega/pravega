package io.pravega.client.byteStream;

import com.google.common.annotations.Beta;

public interface ByteStreamClient {
    
    @Beta
    ByteStreamReader createByteStreamReaders(String streamName);
    
    @Beta
    ByteStreamWriter createByteStreamWriter(String streamName);
}
