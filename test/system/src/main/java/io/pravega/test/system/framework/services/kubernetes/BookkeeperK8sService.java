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
import io.pravega.test.system.framework.services.PravegaProperties;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;

@Slf4j
public class BookkeeperK8sService extends AbstractService {

    private final URI zkUri;
    private final PravegaProperties properties;

    public BookkeeperK8sService(final String id, final URI zkUri, PravegaProperties properties) {
        super(id);
        this.zkUri = zkUri;
        this.properties = properties;
    }

    @Override
    public void start(boolean wait) {
        Futures.getAndHandleExceptions(deployPravegaUsingOperator(zkUri, DEFAULT_CONTROLLER_COUNT, DEFAULT_SEGMENTSTORE_COUNT, DEFAULT_BOOKIE_COUNT, properties),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to deploy pravega operator/pravega services", t));
        if (wait) {
            Futures.getAndHandleExceptions(k8sClient.waitUntilPodIsRunning(NAMESPACE, "component", BOOKKEEPER_LABEL, DEFAULT_BOOKIE_COUNT),
                                           t -> new TestFrameworkException(RequestFailed, "Failed to deploy bookkeeper service, check the operator logs", t));
        }
    }

    @Override
    public void stop() {
        Futures.getAndHandleExceptions(k8sClient.deleteCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA,
                                                                    CUSTOM_RESOURCE_VERSION_PRAVEGA,
                                                                    NAMESPACE,
                                                                    CUSTOM_RESOURCE_PLURAL_PRAVEGA,
                                                                    PRAVEGA_ID),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to stop pravega", t));

    }

    @Override
    public boolean isRunning() {
        return k8sClient.getStatusOfPodWithLabel(NAMESPACE, "component", BOOKKEEPER_LABEL)
                        .thenApply(statuses -> statuses.stream()
                                                       .filter(podStatus -> podStatus.getContainerStatuses()
                                                                                     .stream()
                                                                                     .allMatch(st -> st.getState().getRunning() != null))
                                                       .count())
                        .thenApply(runCount -> runCount >= DEFAULT_BOOKIE_COUNT)
                        .exceptionally(t -> {
                            log.warn("Exception observed while checking status of pods {}. Details: {}", BOOKKEEPER_LABEL, t.getMessage());
                            return false;
                        }).join();
    }

    @Override
    public List<URI> getServiceDetails() {
        //fetch the URI.
        return Futures.getAndHandleExceptions(k8sClient.getStatusOfPodWithLabel(NAMESPACE, "component", BOOKKEEPER_LABEL)
                                                       .thenApply(statuses -> statuses.stream()
                                                                                     .map(s -> URI.create(TCP + s.getPodIP() + ":" + BOOKKEEPER_PORT))
                                                                                     .collect(Collectors.toList())),
                                              t -> new TestFrameworkException(RequestFailed, "Failed to fetch ServiceDetails for bookkeeper", t));
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> scaleService(int newInstanceCount) {
        log.info("Scaling Bookkeeper service to {} instances.", newInstanceCount);
        return k8sClient.getCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA, CUSTOM_RESOURCE_VERSION_PRAVEGA, NAMESPACE, CUSTOM_RESOURCE_PLURAL_PRAVEGA, PRAVEGA_ID)
                        .thenCompose(o -> {
                            Map<String, Object> spec = (Map<String, Object>) (((Map<String, Object>) o).get("spec"));
                            Map<String, Object> pravegaSpec = (Map<String, Object>) spec.get("pravega");
                            Map<String, Object> bookkeeperSpec = (Map<String, Object>) spec.get("bookkeeper");

                            int currentControllerCount = ((Double) pravegaSpec.get("controllerReplicas")).intValue();
                            int currentSegmentStoreCount = ((Double) pravegaSpec.get("segmentStoreReplicas")).intValue();
                            int currentBookkeeperCount = ((Double) bookkeeperSpec.get("replicas")).intValue();
                            log.debug("Current instance counts : Bookkeeper {} Controller {} SegmentStore {}.", currentBookkeeperCount,
                                      currentControllerCount, currentSegmentStoreCount);
                            if (currentBookkeeperCount != newInstanceCount) {
                                return deployPravegaUsingOperator(zkUri, currentControllerCount, currentSegmentStoreCount, newInstanceCount, properties)
                                        .thenCompose(v -> k8sClient.waitUntilPodIsRunning(NAMESPACE, "component", BOOKKEEPER_LABEL, newInstanceCount));
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        });
    }

}
