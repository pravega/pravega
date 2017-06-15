package io.pravega.client.stream;

import com.google.common.annotations.Beta;

@Beta
public interface ReaderGroupMetrics {

    /**
     * Returns the number of bytes between the last recorded position of the readers in the
     * ReaderGroup and the end of the stream. Note: This value may be somewhat delayed.
     */
    long unreadBytes();
    
}
