package com.emc.logservice.server;

import com.emc.logservice.common.CallbackHelpers;
import com.emc.logservice.common.Exceptions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Helps register multiple fault handlers and invoking them all at once.
 */
public class FaultHandlerRegistry {
    private final List<Consumer<Throwable>> handlers;

    /**
     * Creates a new instance of the FaultHandlerRegistry.
     */
    public FaultHandlerRegistry() {
        this.handlers = new ArrayList<>();
    }

    /**
     * Registers a new Fault Handler.
     *
     * @param handler The Fault Handler to register.
     * @throws NullPointerException If handler is null.
     */
    public void register(Consumer<Throwable> handler) {
        Exceptions.throwIfNull(handler, "handler");
        this.handlers.add(handler);
    }

    /**
     * Handles the given error by invoking every registered handler.
     *
     * @param error The error to handle.
     * @throws NullPointerException If error is null.
     */
    public void handle(Throwable error) {
        Exceptions.throwIfNull(error, "error");
        this.handlers.forEach(handler -> CallbackHelpers.invokeSafely(handler, error, null));
    }
}
