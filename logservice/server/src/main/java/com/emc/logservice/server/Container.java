package com.emc.logservice.server;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Defines a Container that can encapsulate a runnable component.
 * Has the ability to Start and Stop processing at any given time.
 */
public interface Container extends AutoCloseable {
    /**
     * Initializes the container, by executing any necessary steps before actual processing can begin.
     * Note that all thrown exceptions will be stored inside the returned CompletableFuture.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation
     * failed, it will contain the causing exception.
     * @throws IllegalStateException If the Container is in an unexpected state when the operation begins. The only
     *                               valid state before is ContainerState.Created. This type of exception may be wrapped
     *                               inside a CompletionException.
     * @throws TimeoutException      If the operation timed out. This type of exception may be wrapped inside a
     *                               CompletionException.
     * @throws CompletionException   If another exception occurred.
     */
    CompletableFuture<Void> initialize(Duration timeout);

    /**
     * Starts processing inside the container.
     * Notes:
     * <ul>
     * <li> All thrown exceptions during startup will be stored inside the returned CompletableFuture.
     * <li>All unhandled exceptions during processing (post-startup) will be reported via the callback provided through
     * the registerFaultHandler method.
     * </ul>
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the container started. If the container failed
     * to start, it will contain the causing exception.
     * @throws IllegalStateException If the Container is in an unexpected state when the operation begins. The only
     *                               valid state before is ContainerState.Initialized. This type of exception may be
     *                               wrapped inside a CompletionException.
     * @throws TimeoutException      If the operation timed out. This type of exception may be wrapped inside a
     *                               CompletionException.
     * @throws CompletionException   If another exception occurred.
     */
    CompletableFuture<Void> start(Duration timeout);

    /**
     * Stops processing inside the container.
     * Note: All thrown exceptions during stopping will be stored inside the returned CompletableFuture.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the container stopped. If the container failed
     * to stop, it will contain the causing exception.
     * @throws IllegalStateException If the Container is in an unexpected state when the operation begins. The only
     *                               valid states before are ContainerState.Started or ContainerState.Stopped. If the
     *                               container was previously in a Stopped state, this operation will have no effect.
     *                               This type of exception may be wrapped inside a CompletionException.
     * @throws TimeoutException      If the operation timed out. This type of exception may be wrapped inside a
     *                               CompletionException.
     * @throws CompletionException   If another exception occurred.
     */
    CompletableFuture<Void> stop(Duration timeout);

    /**
     * Registers the given callback as a fault handler. This will be invoked every time an unhandled exception occurs
     * during normal processing inside the container. This will not be invoked for exceptions thrown during state
     * transitions.
     * <p>
     * Multiple fault handlers can be registered for a single container.
     *
     * @param handler The callback to register.
     */
    void registerFaultHandler(Consumer<Throwable> handler);

    /**
     * Gets a value indicating the current state of the Container.
     *
     * @return
     */
    ContainerState getState();

    /**
     * Gets a value indicating the Id of this container.
     * @return
     */
    String getId();

    @Override
    void close();
}


