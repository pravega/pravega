/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.byteStream;

import com.google.common.annotations.Beta;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;

/**
 * Provides a way to read and write raw byte streams. This API is NOT intended to interoperate with
 * EventStreamReader or EventStreamWriter on the same segment.
 * 
 * It is assumed that any stream passed here to read or write from has already been created by
 * calling
 * {@link StreamManager#createStream(String, String, io.pravega.client.stream.StreamConfiguration)}
 * with a configuration specifying a {@link ScalingPolicy#fixed(int)} scaling policy of 1 segment.
 * 
 * Reader allows direct reading of the bytes from the one and only segment of the specified stream.
 * Writer allows for writing raw bytes (with no headers or wrapping information) to the one and only
 * segment of the stream.
 * @deprecated Use {@link ByteStreamClientFactory} instead
 */
@Deprecated
public interface ByteStreamClient {

    /**
     * Creates a new ByteStreamReader on the specified stream initialized to offset 0.
     * 
     * @param streamName the stream to read from.
     * @return A new ByteStreamReader
     */
    @Beta
    ByteStreamReader createByteStreamReader(String streamName);

    /**
     * Creates a new ByteStreamWriter on the specified stream.
     * 
     * @param streamName The name of the stream to read from.
     * @return A new ByteStreamWriter.
     */
    @Beta
    ByteStreamWriter createByteStreamWriter(String streamName);
}
