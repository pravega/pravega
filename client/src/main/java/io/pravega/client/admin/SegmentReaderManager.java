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
package io.pravega.client.admin;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.impl.SegmentReaderManagerImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import lombok.val;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Used to create and manage segment reader.
 *
 * @param <T> The type of the events written to this segment.
 */
public interface SegmentReaderManager<T> extends AutoCloseable {

    /**
     * Creates a new instance of SegmentReaderManager.
     *
     * @param config Configuration for the client.
     * @param deserializer A deserializer to be used to parse events
     * @param <T> The type of the events written to this segment.
     * @return Instance of StreamReaderManager implementation.
     */
    static <T> SegmentReaderManager<T> create(ClientConfig config, Serializer<T> deserializer) {
        val connectionFactory = new SocketConnectionFactoryImpl(config);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                connectionFactory.getInternalExecutor());
        return new SegmentReaderManagerImpl<>(controller, config, connectionFactory, deserializer);
    }

    /**
     * This api returns a list of SegmentReader based on the provided streamcut.
     * For StreamCut.UNBOUNDED it fetches headStreamCut and then returns list of SegmentReader
     * @param stream Name of the stream
     * @param startStreamCut StreamCut
     * @return List of SegmentReader
     */
    CompletableFuture<List<SegmentReader<T>>> getSegmentReaders(Stream stream, StreamCut startStreamCut);
    
    /**
     * Closes this segment reader manager. No further methods may be called after close.
     * This will free any resources associated with the reader.
     */
    @Override
    void close();
    
}
