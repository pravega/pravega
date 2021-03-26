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
package io.pravega.segmentstore.server;

import com.google.common.util.concurrent.Service;

/**
 * Defines a Container that can encapsulate a runnable component.
 * Has the ability to Start and Stop processing at any given time.
 */
public interface Container extends Service, AutoCloseable {
    /**
     * Gets a value indicating the Id of this container.
     * @return The Id of this container.
     */
    int getId();

    /**
     * Gets a value indicating whether the Container is in an Offline state. When in such a state, even if state() == Service.State.RUNNING,
     * all operations invoked on it will fail with ContainerOfflineException.
     *
     * @return True if the Container is Offline, false if Online.
     */
    boolean isOffline();


    @Override
    void close();
}


