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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import lombok.val;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Used to create and manage segment reader.
 */
public interface SegmentReaderManager extends AutoCloseable {

    /**
     * Creates a new instance of SegmentReaderManager.
     *
     * @param config Configuration for the client.
     * @return Instance of Stream Manager implementation.
     */
    static SegmentReaderManager create(ClientConfig config) {
        val connectionFactory = new SocketConnectionFactoryImpl(config);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                connectionFactory.getInternalExecutor());
        return new SegmentReaderManagerImpl(controller, config, connectionFactory);
    }

    CompletableFuture<List<Long>> getSegmentReaders(Stream stream, StreamCut startStreamCut);
    
    /**
     * Closes this reader. No further methods may be called after close.
     * This will free any resources associated with the reader.
     */
    @Override
    void close();
    
}
