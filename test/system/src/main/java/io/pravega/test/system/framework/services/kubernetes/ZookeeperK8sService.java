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

/**
 * Manage Zookeeper service on K8s cluster.
 */
@Slf4j
public class ZookeeperK8sService extends AbstractService {

    private static final String CUSTOM_RESOURCE_GROUP = "zookeeper.pravega.io";
    private static final String CUSTOM_RESOURCE_VERSION = "v1beta1";
    private static final String CUSTOM_RESOURCE_PLURAL = "zookeeperclusters";
    private static final String CUSTOM_RESOURCE_KIND = "ZookeeperCluster";
    private static final int DEFAULT_INSTANCE_COUNT = 1; // number of zk instances.
    private static final String ZOOKEEPER_IMAGE_NAME = System.getProperty("zookeeperImageName", "zookeeper");
    private static final String PRAVEGA_ZOOKEEPER_IMAGE_VERSION = System.getProperty("zookeeperImageVersion", "latest");

    public ZookeeperK8sService(String id) {
        super(id);
    }

    @Override
    public void start(boolean wait) {
        Futures.getAndHandleExceptions(k8sClient.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP, CUSTOM_RESOURCE_VERSION,
                NAMESPACE, CUSTOM_RESOURCE_PLURAL,
                getZookeeperDeployment(getID(), DEFAULT_INSTANCE_COUNT)),
                t -> new TestFrameworkException(RequestFailed, "Failed to deploy zookeeper operator/service", t));
        if (wait) {
            Futures.getAndHandleExceptions(k8sClient.waitUntilPodIsRunning(NAMESPACE, "app", getID(), DEFAULT_INSTANCE_COUNT),
                    t -> new TestFrameworkException(RequestFailed, "Failed to deploy zookeeper service", t));
        }
    }


    @Override
    public void stop() {
        Futures.getAndHandleExceptions(k8sClient.deleteCustomObject(CUSTOM_RESOURCE_GROUP, CUSTOM_RESOURCE_VERSION, NAMESPACE, CUSTOM_RESOURCE_PLURAL, getID()),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to stop zookeeper service", t));
    }

    @Override
    public void clean() {
    }

    @Override
    public boolean isRunning() {

        return k8sClient.getStatusOfPodWithLabel(NAMESPACE, "app", getID())
                        .thenApply(statuses -> statuses.stream()
                                                      .filter(podStatus -> podStatus.getContainerStatuses()
                                                                                    .stream()
                                                                                    .allMatch(st -> st.getState().getRunning() != null))
                                                      .count() * 2 - statuses.size())
                        .thenApply(netCount -> netCount > 0)
                        .exceptionally(t -> {
                           log.warn("Exception observed while checking status of pod: {}. Details: {} ", getID(), t.getMessage());
                           return false;
                       }).join();
    }

    @Override
    public List<URI> getServiceDetails() {
        // Fetch the URI.
        return Futures.getAndHandleExceptions(k8sClient.getStatusOfPodWithLabel(NAMESPACE, "app", getID())
                                                       .thenApply(statuses -> statuses.stream().map(s -> URI.create(TCP + s.getPodIP() + ":" + ZKPORT))
                                                                                     .collect(Collectors.toList())),
                                              t -> new TestFrameworkException(RequestFailed, "Failed to fetch ServiceDetails for Zookeeper", t));
    }

    @Override
    public CompletableFuture<Void> scaleService(int instanceCount) {
        // Update the instance count.
        // Request operator to deploy zookeeper nodes.
        return k8sClient.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP, CUSTOM_RESOURCE_VERSION, NAMESPACE, CUSTOM_RESOURCE_PLURAL,
                                                     getZookeeperDeployment(getID(), instanceCount))
                        .thenCompose(v -> k8sClient.waitUntilPodIsRunning(NAMESPACE, "app", getID(), instanceCount));
    }

    private Map<String, Object> getZookeeperDeployment(final String deploymentName, final int clusterSize) {
        return ImmutableMap.<String, Object>builder()
                .put("apiVersion", "zookeeper.pravega.io/v1beta1")
                .put("kind", CUSTOM_RESOURCE_KIND)
                .put("metadata", ImmutableMap.of("name", deploymentName))
                .put("spec", ImmutableMap.builder().put("image",  getImageSpec(DOCKER_REGISTRY + PREFIX + "/" + ZOOKEEPER_IMAGE_NAME, PRAVEGA_ZOOKEEPER_IMAGE_VERSION))
                                         .put("replicas", clusterSize)
                                         .put("persistence", ImmutableMap.of("reclaimPolicy", "Delete"))
                                         .put("pod", ImmutableMap.of("resources", getZookeeperResources()))
                                         .build())
                .build();
    }

    private Map<String, Object> getZookeeperResources() {
        return ImmutableMap.<String, Object>builder()
                           .put("limits", ImmutableMap.builder()
                                                      .put("cpu", "400m")
                                                      .put("memory", "2Gi")
                                                      .build())
                           .put("requests", ImmutableMap.builder()
                                                        .put("cpu", "200m")
                                                        .put("memory", "1Gi")
                                                        .build())
                           .build();
    }
}
