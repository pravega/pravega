package com.emc.logservice.server;

import com.google.common.util.concurrent.Service;

import java.util.function.Consumer;

/**
 * Service Listeners that invokes callbacks when the monitored service terminates or fails.
 */
public class ServiceFailureListener extends Service.Listener {
    //region Members

    private final Runnable terminatedCallback;
    private final Consumer<Throwable> failureCallback;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceFailureListener class.
     *
     * @param terminatedCallback An optional callback that will be invoked when the monitored service terminates.
     * @param failureCallback    An optional callback that will be invoked when the monitored service fails. The argument
     *                           to this callback is the exception that caused the service to fail.
     */
    public ServiceFailureListener(Runnable terminatedCallback, Consumer<Throwable> failureCallback) {
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
}