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
package io.pravega.client.state;

/**
 * A constructor for a StateT object.
 * 
 * @param <StateT> A revisioned object that updates to are coordinated with a {@link StateSynchronizer}.
 */
public interface InitialUpdate<StateT extends Revisioned> extends Update<StateT> {
    
    /**
     * Returns an object of type StateT with the provided revision.
     * @param scopedStreamName The name of the stream that this state is associated with.
     * @param revision The revision to use
     * @return A revisioned state object
     */
    StateT create(String scopedStreamName, Revision revision);
    
    @Override
    default StateT applyTo(StateT oldState, Revision newRevision) {
        return create(oldState.getScopedStreamName(), newRevision);
    }
}
