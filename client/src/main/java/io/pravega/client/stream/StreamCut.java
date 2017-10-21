package io.pravega.client.stream;

import io.pravega.client.stream.impl.StreamCutInternal;

/**
 * A set of segment/offset pairs for a single stream that represent a consistent position in the
 * stream. (IE: Segment 1 and 2 will not both appear in the set if 2 succeeds 1, and if 0 appears
 * and is responsible for keyspace 0-0.5 then other segments covering the range 0.5-1.0 will also be
 * included.)
 */
public interface StreamCut {

    /**
     * Gets the stream for which this cut was generated.
     * @return
     */
    public Stream getStream();
    
    /**
     * Used internally. Do not call.
     *
     * @return Implementation of StreamCut object interface
     */
    StreamCutInternal asImpl();
    
}
