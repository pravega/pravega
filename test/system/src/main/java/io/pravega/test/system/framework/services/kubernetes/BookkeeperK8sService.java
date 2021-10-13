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
public class BookkeeperK8sService extends AbstractService {

    private final URI zkUri;
    private final ImmutableMap<String, String> properties;

    public BookkeeperK8sService(final String id, final URI zkUri, final ImmutableMap<String, String> properties) {
        super(id);
        this.zkUri = zkUri;
        this.properties = properties;
    }

    @Override
    public void start(boolean wait) {
        Futures.getAndHandleExceptions(deployBookkeeperCluster(zkUri, DEFAULT_BOOKIE_COUNT, properties),
                t -> new TestFrameworkException(RequestFailed, "Failed to deploy bookkeeper operator/pravega services", t));
        if (wait) {
            Futures.getAndHandleExceptions(k8sClient.waitUntilPodIsRunning(NAMESPACE, "component", BOOKKEEPER_LABEL, DEFAULT_BOOKIE_COUNT),
                    t -> new TestFrameworkException(RequestFailed, "Failed to deploy bookkeeper service, check the operator logs", t));
        }
    }

    @Override
    public void stop() {
        Futures.getAndHandleExceptions(k8sClient.deleteCustomObject(CUSTOM_RESOURCE_GROUP_BOOKKEEPER,
                CUSTOM_RESOURCE_VERSION_BOOKKEEPER,
                NAMESPACE,
                CUSTOM_RESOURCE_PLURAL_BOOKKEEPER,
                BOOKKEEPER_ID),
                t -> new TestFrameworkException(RequestFailed, "Failed to stop bookkeeper", t));

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
        return k8sClient.getCustomObject(CUSTOM_RESOURCE_GROUP_BOOKKEEPER, CUSTOM_RESOURCE_VERSION_BOOKKEEPER, NAMESPACE, CUSTOM_RESOURCE_PLURAL_BOOKKEEPER, BOOKKEEPER_ID)
                .thenCompose(o -> {
                    Map<String, Object> spec = (Map<String, Object>) (((Map<String, Object>) o).get("spec"));
                    int currentBookkeeperCount = ((Double) spec.get("replicas")).intValue();
                    log.info("Expected instance counts: Bookkeeper {} . Current instance counts : Bookkeeper {} .", newInstanceCount, currentBookkeeperCount);
                    if (currentBookkeeperCount != newInstanceCount) {
                        final Map<String, Object> patchedSpec = buildPatchedBookkeeperClusterSpec("replicas", newInstanceCount);
                        return k8sClient.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP_BOOKKEEPER, CUSTOM_RESOURCE_VERSION_BOOKKEEPER, NAMESPACE, CUSTOM_RESOURCE_PLURAL_BOOKKEEPER, patchedSpec)
                                .thenCompose(v -> k8sClient.waitUntilPodIsRunning(NAMESPACE, "component", BOOKKEEPER_LABEL, newInstanceCount));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }
}