/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services.kubernetes;

import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.TestFrameworkException;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;

@Slf4j
public class PravegaSegmentStoreService extends AbstractService {

    private final URI zkUri;

    public PravegaSegmentStoreService(final String id, final URI zkUri) {
        super(id);
        this.zkUri = zkUri;
    }

    @Override
    public void start(boolean wait) {
        Futures.getAndHandleExceptions(deployPravegaUsingOperator(zkUri, DEFAULT_CONTROLLER_COUNT, DEFAULT_SEGMENTSTORE_COUNT, DEFAULT_BOOKIE_COUNT),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to deploy pravega operator/pravega services", t));
        if (wait) {
            Futures.getAndHandleExceptions(k8Client.waitUntilPodIsRunning(NAMESPACE, "component", PRAVEGA_SEGMENTSTORE_LABEL, DEFAULT_SEGMENTSTORE_COUNT),
                                           t -> new TestFrameworkException(RequestFailed, "Failed to deploy pravega-segmentStore service, check the operator logs", t));
        }
    }

    @Override
    public void stop() {
        Futures.getAndHandleExceptions(k8Client.deleteCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA,
                                                                   CUSTOM_RESOURCE_VERSION_PRAVEGA,
                                                                   NAMESPACE,
                                                                   CUSTOM_RESOURCE_PLURAL_PRAVEGA,
                                                                   PRAVEGA_ID),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to stop pravega", t));

    }

    @Override
    public void clean() {
    }


    @Override
    public boolean isRunning() {
        return k8Client.getStatusOfPodWithLabel(NAMESPACE, "component", PRAVEGA_SEGMENTSTORE_LABEL)
                       .thenApply(statuses -> statuses.stream()
                                                      .filter(podStatus -> podStatus.getContainerStatuses()
                                                                                    .stream()
                                                                                    .allMatch(st -> st.getState().getRunning() != null))
                                                      .count())
                       .thenApply(runCount -> runCount == DEFAULT_SEGMENTSTORE_COUNT)
                       .exceptionally(t -> {
                           log.warn("Exception observed while checking status of pods " + PRAVEGA_SEGMENTSTORE_LABEL, t);
                           return false;
                       }).join();
    }

    @Override
    public List<URI> getServiceDetails() {
        //fetch the URI.
        return Futures.getAndHandleExceptions(k8Client.getStatusOfPodWithLabel(NAMESPACE, "component", PRAVEGA_SEGMENTSTORE_LABEL)
                                                      .thenApply(statuses -> statuses.stream()
                                                                                     .map(s -> URI.create(TCP + s.getPodIP() + ":" + SEGMENTSTORE_PORT))
                                                                                     .collect(Collectors.toList())),
                                              t -> new TestFrameworkException(RequestFailed, "Failed to fetch ServiceDetails for pravega-segmentstore", t));
    }

    @Override
    public CompletableFuture<Void> scaleService(int instanceCount) {
        return null;
    }

}
