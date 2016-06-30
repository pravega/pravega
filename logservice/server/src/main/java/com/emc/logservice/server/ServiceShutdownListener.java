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

package com.emc.logservice.server;

import com.google.common.util.concurrent.Service;

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
     * @param service       The service to monitor.
     * @param throwIfFailed Throw the resulting exception if the service ended up in a FAILED state.
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
}