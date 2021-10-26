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
package io.pravega.client;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import lombok.val;

/**
 * Used to create Writers and Readers operating on a Byte Stream.
 * 
 * The byteStreamClient can create readers and writers that work on a stream of bytes. The stream
 * must be pre-created with a single fixed segment. Sharing a stream between the byte stream API and
 * the Event stream readers/writers will CORRUPT YOUR DATA in an unrecoverable way.
 */
@Beta
public interface ByteStreamClientFactory extends AutoCloseable {

    /**
     * Creates a new instance of ByteStreamClientFactory.
     *
     * @param scope The scope string.
     * @param config Configuration for the client.
     * @return Instance of ByteStreamClientFactory implementation.
     */
    static ByteStreamClientFactory withScope(String scope, ClientConfig config) {
        val connectionFactory = new SocketConnectionFactoryImpl(config);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                           connectionFactory.getInternalExecutor());
        val connectionPool = new ConnectionPoolImpl(config, Preconditions.checkNotNull(connectionFactory));
        val inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, connectionPool);
        val outputStreamFactory = new SegmentOutputStreamFactoryImpl(controller, connectionPool);
        val metaStreamFactory = new SegmentMetadataClientFactoryImpl(controller, connectionPool);
        return new ByteStreamClientImpl(scope, controller, connectionPool, inputStreamFactory, outputStreamFactory, metaStreamFactory);
    }

    /**
     * Creates a new ByteStreamReader on the specified stream initialized with the last offset which was passed to
     * ByteStreamWriter::truncateDataBefore(offset), or 0 if truncateDataBefore has not ever been called on this stream.
     *
     * The first byte read from the return value of this method will be the first available byte in the stream,
     * considering any possible truncation.
     * 
     * @param streamName the stream to read from.
     * @return A new ByteStreamReader
     */
    @Beta
    ByteStreamReader createByteStreamReader(String streamName);
    
    /**
     * Creates a new ByteStreamWriter on the specified stream.
     * 
     * @param streamName The name of the stream to write to.
     * @return A new ByteStreamWriter.
     */
    @Beta
    ByteStreamWriter createByteStreamWriter(String streamName);

    /**
     * Closes the ByteStreamClientFactory. This will close any connections created through it.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
