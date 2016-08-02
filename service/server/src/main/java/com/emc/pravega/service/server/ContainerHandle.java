/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server;

import java.util.function.Consumer;

/**
 * Defines a Handle that can be used to control a Container in a SegmentContainerRegistry.
 */
public interface ContainerHandle {
    /**
     * Gets a value indicating the Id of the container.
     *
     * @return
     */
    String getContainerId();

    /**
     * Registers the given callback which will be invoked when the container stopped processing unexpectedly (that is,
     * when the stop was not triggered by a normal sequence of events, such as calling the stop() method).
     *
     * @param handler The callback to invoke. The argument to this callback is the Id of the container.
     */
    void setContainerStoppedListener(Consumer<String> handler);
}
