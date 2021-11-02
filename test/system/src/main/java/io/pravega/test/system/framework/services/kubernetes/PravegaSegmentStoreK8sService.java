/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework.services.kubernetes;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.TestFrameworkException;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;

@Slf4j
public class PravegaSegmentStoreK8sService extends AbstractService {

    private final URI zkUri;
    private final ImmutableMap<String, String> properties;

    public PravegaSegmentStoreK8sService(final String id, final URI zkUri, ImmutableMap<String, String> properties) {
        super(id);
        this.zkUri = zkUri;
        this.properties = properties;
    }

    @Override
    public void start(boolean wait) {
        Futures.getAndHandleExceptions(deployPravegaOnlyCluster(zkUri, DEFAULT_CONTROLLER_COUNT, DEFAULT_SEGMENTSTORE_COUNT, properties),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to deploy pravega operator/pravega services", t));
        if (wait) {
            Futures.getAndHandleExceptions(k8sClient.waitUntilPodIsRunning(NAMESPACE, "component", PRAVEGA_SEGMENTSTORE_LABEL, DEFAULT_SEGMENTSTORE_COUNT),
                                           t -> new TestFrameworkException(RequestFailed, "Failed to deploy pravega-segmentStore service, check the operator logs", t));
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
        return k8sClient.getStatusOfPodWithLabel(NAMESPACE, "component", PRAVEGA_SEGMENTSTORE_LABEL)
                        .thenApply(statuses -> statuses.stream()
                                                      .filter(podStatus -> podStatus.getContainerStatuses()
                                                                                    .stream()
                                                                                    .allMatch(st -> st.getState().getRunning() != null))
                                                      .count())
                        .thenApply(runCount -> runCount >= DEFAULT_SEGMENTSTORE_COUNT)
                        .exceptionally(t -> {
                            log.warn("Exception observed while checking status of pods {}. Details: {}", PRAVEGA_SEGMENTSTORE_LABEL,
                                     t.getMessage());
                            return false;
                        }).join();
    }

    @Override
    public List<URI> getServiceDetails() {
        //fetch the URI.
        return Futures.getAndHandleExceptions(k8sClient.getStatusOfPodWithLabel(NAMESPACE, "component", PRAVEGA_SEGMENTSTORE_LABEL)
                                                       .thenApply(statuses -> statuses.stream()
                                                                                     .map(s -> URI.create(TCP + s.getPodIP() + ":" + SEGMENTSTORE_PORT))
                                                                                     .collect(Collectors.toList())),
                                              t -> new TestFrameworkException(RequestFailed, "Failed to fetch ServiceDetails for pravega-segmentstore", t));
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> scaleService(int newInstanceCount) {
        log.info("Scaling Pravega Segment store service to {} instances.", newInstanceCount);
        return k8sClient.getCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA, CUSTOM_RESOURCE_VERSION_PRAVEGA, NAMESPACE, CUSTOM_RESOURCE_PLURAL_PRAVEGA, PRAVEGA_ID)
                        .thenCompose(o -> {
                           Map<String, Object> spec = (Map<String, Object>) (((Map<String, Object>) o).get("spec"));
                           Map<String, Object> pravegaSpec = (Map<String, Object>) spec.get("pravega");

                           int currentControllerCount = ((Double) pravegaSpec.get("controllerReplicas")).intValue();
                           int currentSegmentStoreCount = ((Double) pravegaSpec.get("segmentStoreReplicas")).intValue();
                           log.info("Current instance counts : Controller {} SegmentStore {}.",
                                     currentControllerCount, currentSegmentStoreCount);
                           if (currentSegmentStoreCount != newInstanceCount) {
                               final Map<String, Object> patchedSpec = buildPatchedPravegaClusterSpec("segmentStoreReplicas", newInstanceCount, "pravega");
                               return k8sClient.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA, CUSTOM_RESOURCE_VERSION_PRAVEGA, NAMESPACE, CUSTOM_RESOURCE_PLURAL_PRAVEGA, patchedSpec)
                                       .thenCompose(v -> k8sClient.waitUntilPodIsRunning(NAMESPACE, "component", PRAVEGA_SEGMENTSTORE_LABEL, newInstanceCount));
                           } else {
                               return CompletableFuture.completedFuture(null);
                           }
                       });
    }

}
