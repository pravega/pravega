/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;


import com.emc.pravega.ScopeManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;

import com.google.common.annotations.VisibleForTesting;

import java.net.URI;

public class ScopeManagerImpl implements ScopeManager {

    private final Controller controller;

    public ScopeManagerImpl(URI controllerUri) {
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
    }

    @VisibleForTesting
    public ScopeManagerImpl(Controller controller) {
        this.controller = controller;
    }

    @Override
    public void createScope(String name) {
        CreateScopeStatus status = FutureHelpers.getAndHandleExceptions(controller.createScope(name),
                                                                        RuntimeException::new);
    }

    @Override
    public void deleteScope(String name) {
        DeleteScopeStatus status = FutureHelpers.getAndHandleExceptions(controller.deleteScope(name),
                                                                        RuntimeException::new);
    }

    @Override
    public void close() throws Exception {
        // Nothing to do
    }
}
