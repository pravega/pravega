/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import java.util.function.Consumer;

/**
 * Defines a Handle that can be used to control a Container in a SegmentContainerRegistry.
 */
public interface ContainerHandle {
    /**
     * Gets a value indicating the Id of the container.
     * @return The Id of the container.
     */
    int getContainerId();

    /**
     * Registers the given callback which will be invoked when the container stopped processing unexpectedly (that is,
     * when the stop was not triggered by a normal sequence of events, such as calling the stop() method).
     *
     * @param handler The callback to invoke. The argument to this callback is the Id of the container.
     */
    void setContainerStoppedListener(Consumer<Integer> handler);
}
