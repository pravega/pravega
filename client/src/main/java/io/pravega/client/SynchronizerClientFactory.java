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

import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import lombok.val;

/**
 * Used to create StateSynchronizer and RevisionedStreamClient objects which allow for 
 * reading and writing data from a pre-configured stream with strong consistency guarantees.
 */
public interface SynchronizerClientFactory extends AutoCloseable {

    /**
     * Creates a new instance of Client Factory.
     *
     * @param scope The scope string.
     * @param config Configuration for the client.
     * @return Instance of ClientFactory implementation.
     */
    static SynchronizerClientFactory withScope(String scope, ClientConfig config) {
        // Change the max number of number of allowed connections to the segment store to 1.
        val updatedConfig = config.toBuilder()
                .maxConnectionsPerSegmentStore(1)
                .enableTlsToSegmentStore(config.isEnableTlsToSegmentStore())
                .enableTlsToController(config.isEnableTlsToController())
                .build();
        val connectionFactory = new SocketConnectionFactoryImpl(updatedConfig, 1);
        return new ClientFactoryImpl(scope, new ControllerImpl(ControllerImplConfig.builder().clientConfig(updatedConfig).build(),
                connectionFactory.getInternalExecutor()), updatedConfig, connectionFactory);
    }

    /**
     * Creates a new RevisionedStreamClient that will work with the specified stream.
     *
     * @param streamName The name of the stream for the synchronizer.
     * @param serializer The serializer for updates.
     * @param config The client configuration.
     * @param <T> The type of events.
     * @return Revisioned stream client.
     */
    <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
            SynchronizerConfig config);
    
    /**
     * Creates a new StateSynchronizer that will work on the specified stream.
     *
     * @param <StateT> The type of the state being synchronized.
     * @param <UpdateT> The type of the updates being written.
     * @param <InitT> The type of the initial update used.
     * @param streamName The name of the stream for the synchronizer.
     * @param updateSerializer The serializer for updates.
     * @param initSerializer The serializer for the initial update.
     * @param config The synchronizer configuration.
     * @return Newly created StateSynchronizer that will work on the given stream.
     */
    <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>>
    StateSynchronizer<StateT> createStateSynchronizer(String streamName,
                                                      Serializer<UpdateT> updateSerializer,
                                                      Serializer<InitT> initSerializer,
                                                      SynchronizerConfig config);

    /**
     * Closes the client factory. This will close any connections created through it.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

}
