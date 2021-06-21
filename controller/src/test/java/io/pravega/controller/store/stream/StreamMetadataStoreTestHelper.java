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
package io.pravega.controller.store.stream;

import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.controller.store.TestOperationContext;
import io.pravega.controller.store.stream.records.StateRecord;

import java.util.Optional;

public class StreamMetadataStoreTestHelper {
    public static PersistentStreamBase getStream(StreamMetadataStore store, String scopeName, String streamName) {
        return (PersistentStreamBase) ((AbstractStreamMetadataStore) store)
                .newStream(scopeName, streamName);
    }
    
    public static void partiallyCreateStream(StreamMetadataStore store, String scopeName, String streamName, 
                                             Optional<Long> creationTime, boolean createState) {
        addStreamToScope(store, scopeName, streamName);
        OperationContext context = getContext();
        PersistentStreamBase stream = getStream(store, scopeName, streamName);
        stream.createStreamMetadata(context).join();
        creationTime.map(x -> stream.storeCreationTimeIfAbsent(x, context).join());
        if (createState) {
            stream.createStateIfAbsent(StateRecord.builder().state(State.CREATING).build(), context).join();
        }
    }

    private static OperationContext getContext() {
        return new TestOperationContext();
    }

    public static void addStreamToScope(StreamMetadataStore store, String scopeName, String streamName) {
        OperationContext context = getContext();
        if (store instanceof InMemoryStreamMetadataStore) {
            ((InMemoryStreamMetadataStore) store).addStreamObjToScope(scopeName, streamName);
        } else if (store instanceof PravegaTablesStreamMetadataStore) {
            ((PravegaTablesScope) ((PravegaTablesStreamMetadataStore) store).getScope(scopeName, context))
                    .addStreamToScope(streamName, context).join();
        } else if (store instanceof ZKStreamMetadataStore) {
            ((ZKStream) ((ZKStreamMetadataStore) store).getStream(scopeName, streamName, null))
                    .createStreamMetadata(context).join();
        }
    }
}
