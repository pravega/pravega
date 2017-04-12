/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega;

import com.emc.pravega.shared.common.concurrent.FutureHelpers;
import com.emc.pravega.framework.services.PravegaControllerService;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.stream.impl.ControllerImpl;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract scale tests. This contains all the shared methods used for auto scale related tests.
 */
@Slf4j
abstract class AbstractScaleTests {
    private final AtomicReference<ClientFactory> clientFactoryRef = new AtomicReference<>();
    private final AtomicReference<ControllerImpl> controllerRef = new AtomicReference<>();

    ClientFactory getClientFactory(final String scope) {
        if (clientFactoryRef.get() == null) {
            clientFactoryRef.set(ClientFactory.withScope(scope, getControllerURI()));
        }
        return clientFactoryRef.get();
    }

    ControllerImpl getController(final URI controllerUri) {
        if (controllerRef.get() == null) {
            log.debug("Controller uri: {}", controllerUri);

            controllerRef.set(new ControllerImpl(controllerUri));
        }
        return controllerRef.get();
    }

    URI getControllerURI() {
        Service conService = new PravegaControllerService("controller", null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }

    void recordResult(final CompletableFuture<Void> scaleTestResult, final String testName) {
        FutureHelpers.getAndHandleExceptions(scaleTestResult.handle((r, e) -> {
            if (e != null) {
                log.error("test {} failed with exception {}", testName, e);
            } else {
                log.debug("test {} succeed", testName);
            }
            return null;
        }), RuntimeException::new);
    }

    // Exception to indicate that the scaling operation did not happen.
    // We need to retry operation to check scaling on this exception.
    class ScaleOperationNotDoneException extends RuntimeException {
    }

}
