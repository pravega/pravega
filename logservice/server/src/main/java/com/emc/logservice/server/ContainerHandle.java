package com.emc.logservice.server;

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
