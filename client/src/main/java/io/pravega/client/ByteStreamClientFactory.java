/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import com.google.common.annotations.Beta;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import lombok.val;

/**
 * Used to create Writers and Readers operating on a Byte Stream.
 * 
 * The byteStreamClient can create readers and writers that work on a stream of bytes. The stream
 * must be pre-created with a single fixed segment. Sharing a stream between the byte stream API and
 * the Event stream readers/writers will CORRUPT YOUR DATA in an unrecoverable way.
 */
@Beta
public interface ByteStreamClientFactory {

    /**
     * Creates a new instance of Client Factory.
     *
     * @param scope The scope string.
     * @param config Configuration for the client.
     * @return Instance of ClientFactory implementation.
     */
    static ByteStreamClientFactory withScope(String scope, ClientConfig config) {
        val connectionFactory = new ConnectionFactoryImpl(config);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                           connectionFactory.getInternalExecutor());
        SegmentInputStreamFactoryImpl inFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        SegmentOutputStreamFactoryImpl outFactory = new SegmentOutputStreamFactoryImpl(controller, connectionFactory);
        SegmentMetadataClientFactoryImpl metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
        return new ByteStreamClientImpl(scope, controller, inFactory, outFactory, metaFactory);
    }

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
