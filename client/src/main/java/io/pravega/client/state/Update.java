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
 * An update to a StateT object coordinated via a {@link StateSynchronizer}.
 * 
 * @param <StateT> The type of the object being updated
 */
public interface Update<StateT extends Revisioned> {

    /**
     * Return an object of type StateT that is the same as oldState with this update applied to it.
     * Invoking {@link Revisioned#getRevision()} on the result should return newRevision.
     * 
     * @param oldState The state to which should be used as the basis for the new state.
     * @param newRevision The revision for the new state.
     * @return A state that represents a state with this update applied to it. (If StateT is mutable
     *         and the update is done in-place this is the same object as oldState)
     */
    StateT applyTo(StateT oldState, Revision newRevision);

}
