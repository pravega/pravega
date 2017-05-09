/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import io.pravega.common.TimeoutTimer;
import com.google.common.util.concurrent.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Service Listeners that invokes callbacks when the monitored service terminates or fails.
 */
public class ServiceShutdownListener extends Service.Listener {
    //region Members

    private final Runnable terminatedCallback;
    private final Consumer<Throwable> failureCallback;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceShutdownListener class.
     *
     * @param terminatedCallback An optional callback that will be invoked when the monitored service terminates.
     * @param failureCallback    An optional callback that will be invoked when the monitored service fails. The argument
     *                           to this callback is the exception that caused the service to fail.
     */
    public ServiceShutdownListener(Runnable terminatedCallback, Consumer<Throwable> failureCallback) {
        this.terminatedCallback = terminatedCallback;
        this.failureCallback = failureCallback;
    }

    //endregion

    @Override
    public void terminated(Service.State from) {
        if (this.terminatedCallback != null) {
            this.terminatedCallback.run();
        }
    }

    @Override
    public void failed(Service.State from, Throwable failure) {
        if (this.failureCallback != null) {
            this.failureCallback.accept(failure);
        }
    }

    /**
     * Awaits for the given Service to shut down, whether normally or exceptionally.
     *
     * @param service       The store to monitor.
     * @param throwIfFailed Throw the resulting exception if the store ended up in a FAILED state.
     */
    public static void awaitShutdown(Service service, boolean throwIfFailed) {
        try {
            service.awaitTerminated();
        } catch (IllegalStateException ex) {
            if (throwIfFailed || service.state() != Service.State.FAILED) {
                throw ex;
            }
        }
    }

    /**
     * Awaits for the given Service to shut down, whether normally or exceptionally.
     *
     * @param service       The store to monitor.
     * @param timeout       The maximum amount of time to wait for the shutdown.
     * @param throwIfFailed Throw the resulting exception if the store ended up in a FAILED state.
     * @throws TimeoutException If the store did not shut down within the specified amount of time. This exception is
     *                          thrown regardless of the value passed in as throwIfFailed.
     */
    public static void awaitShutdown(Service service, Duration timeout, boolean throwIfFailed) throws TimeoutException {
        try {
            service.awaitTerminated(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (IllegalStateException ex) {
            if (throwIfFailed || service.state() != Service.State.FAILED) {
                throw ex;
            }
        }
    }

    /**
     * Awaits for the given Services to shut down, whether normally or exceptionally.
     *
     * @param services      The services to monitor.
     * @param throwIfFailed Throw an IllegalStateException if any of the services ended up in a FAILED state.
     * @param <T>           The type of the Collection's elements.
     */
    public static <T extends Service> void awaitShutdown(Collection<T> services, boolean throwIfFailed) {
        int failureCount = 0;
        for (Service service : services) {
            try {
                service.awaitTerminated();
            } catch (IllegalStateException ex) {
                if (throwIfFailed || service.state() != Service.State.FAILED) {
                    failureCount++;
                }
            }
        }

        if (failureCount > 0) {
            throw new IllegalStateException(String.format("%d store(s) could not be shut down.", failureCount));
        }
    }

    /**
     * Awaits for the given Services to shut down, whether normally or exceptionally.
     *
     * @param services      The services to monitor.
     * @param timeout       Timeout for the operation.
     * @param throwIfFailed Throw an IllegalStateException if any of the services ended up in a FAILED state.
     * @param <T>           The type of the Collection's elements.
     * @throws TimeoutException If the store did not shut down within the specified amount of time. This exception is
     *                          thrown regardless of the value passed in as throwIfFailed.
     */
    public static <T extends Service> void awaitShutdown(Collection<T> services, Duration timeout, boolean throwIfFailed) throws TimeoutException {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        int failureCount = 0;
        for (Service service : services) {
            try {
                // We wait on each store individually, making sure we decrease the available timeout. It does not matter
                // if we do it sequentially or in parallel - our contract is to wait for all services within a timeout;
                // if any of the services did not respond within the given time, it is a reason for failure.
                service.awaitTerminated(timer.getRemaining().toMillis(), TimeUnit.MILLISECONDS);
            } catch (IllegalStateException ex) {
                if (throwIfFailed || service.state() != Service.State.FAILED) {
                    failureCount++;
                }
            }
        }

        if (failureCount > 0) {
            throw new IllegalStateException(String.format("%d store(s) could not be shut down.", failureCount));
        }
    }
}