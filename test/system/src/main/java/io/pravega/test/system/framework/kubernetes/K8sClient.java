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
package io.pravega.test.system.framework.kubernetes;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.openapi.apis.ApiextensionsV1Api;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ClusterRole;
import io.kubernetes.client.openapi.models.V1ClusterRoleBinding;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Watch;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.TestFrameworkException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.Futures.exceptionallyExpecting;
import static io.pravega.test.system.framework.TestFrameworkException.Type.ConnectionFailed;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Slf4j
public class K8sClient {

    private static final boolean ALLOW_WATCH_BOOKMARKS = true;
    // Indicates if an object can be returned without completing its initialization.
    private static final int DEFAULT_TIMEOUT_MINUTES = 10; // timeout of http client.
    private static final int RETRY_MAX_DELAY_MS = 1_000; // max time between retries to check if pod has completed.
    private static final int RETRY_COUNT = 50; // Max duration incase of an exception is around 50 * RETRY_MAX_DELAY_MS = 50 seconds.
    private static final int LOG_DOWNLOAD_RETRY_COUNT = 7;
    // Delay before starting to download the logs. The K8s api server responds with error code 400 if immediately requested for log download.
    private static final long LOG_DOWNLOAD_INIT_DELAY_MS = SECONDS.toMillis(20);
    // When present, indicates that modifications should not be persisted. Only valid value is "All", or null.
    private static final String DRY_RUN = null;
    private static final String FIELD_MANAGER = "pravega-k8-client";
    private static final String PRETTY_PRINT = null;
    private final ApiClient client;
    private final PodLogs logUtility;
    // size of the executor is 3 (1 thread is used to watch the pod status, 2 threads for background log copy).
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(3, "pravega-k8s-client");
    private final Retry.RetryWithBackoff retryWithBackoff = Retry.withExpBackoff(1000, 10, RETRY_COUNT, RETRY_MAX_DELAY_MS);
    private final Predicate<Throwable> isConflict = t -> {
        if (t instanceof ApiException && ((ApiException) t).getCode() == CONFLICT.getStatusCode()) {
            log.info("Ignoring Response code {} from KUBERNETES api server", CONFLICT.getStatusCode());
            return true;
        }
        log.error("Exception observed from KUBERNETES api server", t);
        return false;
    };

    K8sClient() {
        this.client = initializeApiClient();
        this.logUtility = new PodLogs();
    }

    /**
     * Create an instance of K8 api client and initialize with the KUBERNETES config. The config used follows the below pattern.
     *      1. If $KUBECONFIG is defined, use that config file.
     *      2. If $HOME/.kube/config can be found, use that.
     *      3. If the in-cluster service account can be found, assume in cluster config.
     *      4. Default to localhost:8080 as a last resort.
     */
    private ApiClient initializeApiClient() {
        ApiClient client;
        try {
            log.debug("Initialize KUBERNETES api client");
            client = Config.defaultClient();
            client.setDebugging(false); // this can be set to true enable http dump.
            client.setHttpClient(client.getHttpClient().newBuilder().readTimeout(DEFAULT_TIMEOUT_MINUTES, TimeUnit.MINUTES).build());
            Configuration.setDefaultApiClient(client);
            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        } catch (IOException e) {
            throw new TestFrameworkException(ConnectionFailed, "Connection to the k8 cluster failed, ensure .kube/config is configured correctly.", e);
        }
        return client;
    }

    /**
     * Method used to create a namespace. This blocks until the namespace is created.
     * @param namespace Namespace to be created.
     * @return V1Namespace.
     */
    @SneakyThrows(ApiException.class)
    public V1Namespace createNamespace(final String namespace) {
        CoreV1Api api = new CoreV1Api();
        try {
            V1Namespace existing = api.readNamespace(namespace, PRETTY_PRINT);
            if (existing != null) {
                log.info("Namespace {} already exists, ignoring namespace create operation.", namespace);
                return existing;
            }
        } catch (ApiException ignore) {
            // ignore exception and proceed with Namespace creation.
        }

        V1Namespace body = new V1Namespace();
        // Set the required api version and kind of resource
        body.setApiVersion("v1");
        body.setKind("Namespace");

        // Setup the standard object metadata
        V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(namespace);
        body.setMetadata(meta);

        return api.createNamespace(body, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null);
    }

    /**
     * Deploy a pod. This ignores exception when the pod has already been deployed.
     * @param namespace Namespace.
     * @param pod Pod details.
     * @return Future which is completed once the deployemnt has been triggered.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Pod> deployPod(final String namespace, final V1Pod pod) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1Pod> callback = new K8AsyncCallback<>("createPod-" + pod.getMetadata().getName());
        api.createNamespacedPodAsync(namespace, pod, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Method used to fetch the status of a Pod. V1PodStatus also helps to indicate the container status.
     * @param namespace Namespace.
     * @param podName Name of the pod.
     * @return A future representing the status of the pod.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1PodStatus> getStatusOfPod(final String namespace, final String podName) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1PodList> callback = new K8AsyncCallback<>("listPods");
        api.listNamespacedPodAsync(namespace, PRETTY_PRINT, ALLOW_WATCH_BOOKMARKS, null, null, "POD_NAME=" + podName, null,
                                  null, null, null, false, callback);
        return callback.getFuture()
                .thenApply(v1PodList -> {
                    Optional<V1Pod> vpod = v1PodList.getItems().stream().filter(v1Pod -> v1Pod.getMetadata().getName().equals(podName) &&
                            v1Pod.getMetadata().getNamespace().equals(namespace)).findFirst();
                    return vpod.map(V1Pod::getStatus).orElseThrow(() -> new RuntimeException("pod not found" + podName));
                });
    }

    /**
     * Method to fetch the status of all pods which match a label.
     * @param namespace Namespace on which the pod(s) reside.
     * @param labelName Name of the label.
     * @param labelValue Value of the label.
     * @return Future representing the list of pod status.
     */
    public CompletableFuture<List<V1PodStatus>> getStatusOfPodWithLabel(final String namespace, final String labelName, final String labelValue) {
        return getPodsWithLabel(namespace, labelName, labelValue)
                .thenApply(v1PodList -> {
                    List<V1Pod> podList = v1PodList.getItems();
                    log.info("{} pod(s) found with label {}={}.", podList.size(), labelName, labelValue);
                    return podList.stream().map(V1Pod::getStatus).collect(Collectors.toList());
                });
    }

    /**
     * Method to fetch all pods which match a label.
     * @param namespace Namespace on which the pod(s) reside.
     * @param labelName Name of the label.
     * @param labelValue Value of the label.
     * @return Future representing the list of pod status.
     */
    public CompletableFuture<V1PodList> getPodsWithLabel(String namespace, String labelName, String labelValue) {
        return getPodsWithLabels(namespace, ImmutableMap.of(labelName, labelValue));
    }

    /**
     * Method to fetch all pods which match a set of labels.
     * @param namespace Namespace on which the pod(s) reside.
     * @param labels Name of the label.
     * @return Future representing the list of pod status.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1PodList> getPodsWithLabels(String namespace, Map<String, String> labels) {
        CoreV1Api api = new CoreV1Api();

        log.debug("Current number of http interceptors {}", api.getApiClient().getHttpClient().networkInterceptors().size());

        String labelSelector = labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining());
        K8AsyncCallback<V1PodList> callback = new K8AsyncCallback<>("listPods-" + labels);
        api.listNamespacedPodAsync(namespace, PRETTY_PRINT, ALLOW_WATCH_BOOKMARKS, null, null, labelSelector, null,
                null, null, null, false, callback);
        return callback.getFuture();
    }

    /**
     * Method to fetch all restarted pods which match a label.
     * Note: This method currently supports only one container per pod.
     * @param namespace Namespace on which the pod(s) reside.
     * @param labelName Name of the label.
     * @param labelValue Value of the label.
     * @return Future representing the list of pod status.
     */
    public CompletableFuture<Map<String, V1ContainerStatus>> getRestartedPods(String namespace, String labelName, String labelValue) {
        return getPodsWithLabel(namespace, labelName, labelValue)
                     .thenApply(v1PodList -> v1PodList.getItems().stream()
                             .filter(pod -> !pod.getStatus().getContainerStatuses().isEmpty() &&
                                     (pod.getStatus().getContainerStatuses().get(0).getRestartCount() != 0))
                             .collect(Collectors.toMap(pod -> pod.getMetadata().getName(),
                                     pod -> pod.getStatus().getContainerStatuses().get(0))));
    }

    /**
     * Create a deployment on KUBERNETES, if the deployment is already present then it is ignored.
     * @param namespace Namespace.
     * @param deploy Deployment object.
     * @return A future which represents the creation of the Deployment.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Deployment> createDeployment(final String namespace, final V1Deployment deploy) {
        AppsV1Api api = new AppsV1Api();
        K8AsyncCallback<V1Deployment> callback = new K8AsyncCallback<>("deployment-" + deploy.getMetadata().getName());
        api.createNamespacedDeploymentAsync(namespace, deploy, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Fetch the deployment status.
     * @param deploymentName Name of the deployment
     * @param namespace Namespace where the deployment exists.
     * @return Future representing the Deployement.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Deployment> getDeploymentStatus(final String deploymentName, final String namespace) {
        AppsV1Api api = new AppsV1Api();
        K8AsyncCallback<V1Deployment> callback = new K8AsyncCallback<>("readNamespacedDeployment-" + deploymentName);
        api.readNamespacedDeploymentStatusAsync(deploymentName, namespace, PRETTY_PRINT, callback);
        return callback.getFuture();
    }


    /**
     * Create a Custom object for a Custom Resource Definition (CRD). This is useful while interacting with operators.
     * @param customResourceGroup Custom resource group.
     * @param version Version.
     * @param namespace Namespace.
     * @param plural plural of the CRD.
     * @param request Actual request.
     * @return Future representing the custom object creation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<Object> createCustomObject(String customResourceGroup, String version, String namespace,
                                                        String plural, Map<String, Object> request) {
        CustomObjectsApi api = new CustomObjectsApi();
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("createCustomObject-" + customResourceGroup);
        api.createNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, request, PRETTY_PRINT, null,  null, callback);
        return callback.getFuture();
    }

    /**
     * This is used to update a custom object. This is useful to modify the custom object configuration, number of
     * instances is one type of configuration. If the object does not exist then a new object is created.
     * @param customResourceGroup Custom resource group.
     * @param version version.
     * @param namespace Namespace.
     * @param plural Plural of the CRD.
     * @param request Actual request.
     * @return A Future representing the status of create/update.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<Object> createAndUpdateCustomObject(String customResourceGroup, String version, String namespace,
                                                                 String plural, Map<String, Object> request) {
        CustomObjectsApi api = new CustomObjectsApi();
        //Fetch the name of the custom object.
        String name = ((Map<String, String>) request.get("metadata")).get("name");
        return getCustomObject(customResourceGroup, version, namespace, plural, name)
                .thenCompose(o -> {
                    log.info("Instance {} of custom resource {}  exists, update it with the new request", name, customResourceGroup);
                    try {
                        //patch object
                        K8AsyncCallback<Object> cb1 = new K8AsyncCallback<>("patchCustomObject");
                        PatchUtils.patch(CustomObjectsApi.class,
                                 () -> api.patchNamespacedCustomObjectCall(
                                        customResourceGroup,
                                        version,
                                        namespace,
                                        plural,
                                        name,
                                        request,
                                        null,
                                        null,
                                        null,
                                        cb1),
                                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH);
                        return cb1.getFuture();
                    } catch (ApiException e) {
                        throw Exceptions.sneakyThrow(e);
                    }
                }).exceptionally(t -> {
                    log.warn("Exception while trying to fetch instance {} of custom resource {}, try to create it. Details: {}", name,
                            customResourceGroup, t.getMessage());
                    try {
                        //create object
                        K8AsyncCallback<Object> cb = new K8AsyncCallback<>("createCustomObject");
                        api.createNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, request, PRETTY_PRINT, null, null, cb);
                        return cb.getFuture();
                    } catch (ApiException e) {
                        throw Exceptions.sneakyThrow(e);
                    }
                });
    }

    /**
     * Fetch Custom Object for a given custom resource group.
     * @param customResourceGroup Custom resource group.
     * @param version Version.
     * @param namespace Namespace.
     * @param plural Plural of the CRD.
     * @param name Name of the object.
     * @return A future which returns the details of the object. The future completes exceptionally if the object is not present.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<Object> getCustomObject(String customResourceGroup, String version, String namespace,
                                                     String plural, String name) {
        CustomObjectsApi api = new CustomObjectsApi();
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("getCustomObject-" + customResourceGroup);
        api.getNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, name, callback);
        return callback.getFuture();
    }

    /**
     * Delete Custom Object for a given resource group.
     * @param customResourceGroup Custom resource group.
     * @param version Version.
     * @param namespace Namespace.
     * @param plural Plural of the CRD.
     * @param name Name of the object.
     * @return Future which completes once the delete request is accepted.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<Object> deleteCustomObject(String customResourceGroup, String version, String namespace,
                                                        String plural, String name) {

        CustomObjectsApi api = new CustomObjectsApi();
        V1DeleteOptions options = new V1DeleteOptions();
        options.setOrphanDependents(false);
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("getCustomObject-" + customResourceGroup);
        api.deleteNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, name,
                0, false, null, null, options, callback);

        return callback.getFuture();
    }

    /**
     * Delete persistent volume claim.
     * @param namespace Namespace.
     * @param name Persistent volume claim name.
     */
    @SneakyThrows(ApiException.class)
    public void deletePVC(String namespace, String name) {
        CoreV1Api api = new CoreV1Api();
        try {
            api.deleteNamespacedPersistentVolumeClaim(name, namespace, PRETTY_PRINT, DRY_RUN, null, null, null, new V1DeleteOptions());
        } catch (JsonSyntaxException e) {
            // https://github.com/kubernetes-client/java/issues/86
            if (e.getCause() instanceof IllegalStateException) {
                IllegalStateException ise = (IllegalStateException) e.getCause();
                if (ise.getMessage() != null && ise.getMessage().contains("Expected a string but was BEGIN_OBJECT")) {
                    log.debug("Ignoring exception", e);
                    return;
                }
            }
            throw e;
        }
    }

    /**
     * Create a Custom Resource Definition (CRD).
     * @param crd Custom resource defnition.
     * @return A future indicating the status of this operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1CustomResourceDefinition> createCRD(final V1CustomResourceDefinition crd) {
        ApiextensionsV1Api api = new ApiextensionsV1Api();
        K8AsyncCallback<V1CustomResourceDefinition> callback = new K8AsyncCallback<>("create CRD-" + crd.getMetadata().getName());
        api.createCustomResourceDefinitionAsync(crd, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Create a cluster role.
     * @param role Cluster Role.
     * @return A future indicating the status of this operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1ClusterRole> createClusterRole(V1ClusterRole role) {
        RbacAuthorizationV1Api api = new RbacAuthorizationV1Api();
        K8AsyncCallback<V1ClusterRole> callback = new K8AsyncCallback<>("createClusterRole-" + role.getMetadata().getName());
        api.createClusterRoleAsync(role, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Create a role.
     * @param namespace Namespace where the role is created.
     * @param role Role.
     * @return A future indicating the status of this operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Role> createRole(String namespace, V1Role role) {
        RbacAuthorizationV1Api api = new RbacAuthorizationV1Api();
        K8AsyncCallback<V1Role> callback = new K8AsyncCallback<>("createRole-" + role.getMetadata().getName());
        api.createNamespacedRoleAsync(namespace, role, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Create cluster role binding.
     * @param binding The cluster role binding.
     * @return A future indicating the status of the create role binding operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1ClusterRoleBinding> createClusterRoleBinding(V1ClusterRoleBinding binding) {
        RbacAuthorizationV1Api api = new RbacAuthorizationV1Api();
        K8AsyncCallback<V1ClusterRoleBinding> callback = new K8AsyncCallback<>("createClusterRoleBinding");
        api.createClusterRoleBindingAsync(binding, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Create role binding.
     * @param namespace The namespace where the binding should be created.
     * @param binding The cluster role binding.
     * @return A future indicating the status of the create role binding operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1RoleBinding> createRoleBinding(String namespace, V1RoleBinding binding) {
        RbacAuthorizationV1Api api = new RbacAuthorizationV1Api();
        K8AsyncCallback<V1RoleBinding> callback = new K8AsyncCallback<>("createRoleBinding-" + binding.getMetadata().getName());
        api.createNamespacedRoleBindingAsync(namespace, binding, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Create a service account.
     * @param namespace The namespace.
     * @param account Service Account.
     * @return A future indicating the status of create service account operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1ServiceAccount> createServiceAccount(String namespace, V1ServiceAccount account) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1ServiceAccount> callback = new K8AsyncCallback<>("createServiceAccount-" + account.getMetadata().getName());
        api.createNamespacedServiceAccountAsync(namespace, account, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * A method which returns a completed future once a given Pod has completed execution. This is useful to track test execution.
     * This method uses a Watch to track the status of the pod. The maximum wait time is based on the retry configuration.
     * @param namespace Namespace.
     * @param podName Pod name.
     * @return A future which is complete once the pod completes.
     */
    public CompletableFuture<V1ContainerStateTerminated> waitUntilPodCompletes(final String namespace, final String podName) {
        return retryWithBackoff.retryWhen(t -> {
            Throwable ex = Exceptions.unwrap(t);
            //Incase of an IO Exception the Kubernetes client wraps the IOException within a RuntimeException.
            if (ex.getCause() instanceof IOException) {
                // IOException might occur due multiple reasons, one among them is SocketTimeout exception.
                // This is observed on long running pods.
                log.warn("IO Exception while fetching status of pod, will attempt a retry. Details: {}", ex.getMessage());
                return true;
            }
            log.error("Exception while fetching status of pod", ex);
            return false;
        }).runAsync(() -> {
            CompletableFuture<V1ContainerStateTerminated> future = new CompletableFuture<>();
            V1ContainerStateTerminated state = createAWatchAndReturnOnTermination(namespace, podName)
                    .orElseThrow(() -> new RuntimeException("Watch did not return terminated state for pod " + podName));
            future.complete(state);
            return future;
        }, executor);
    }

    /**
     * Create a Watch for a pod and return once the pod has terminated.
     * @param namespace Namespace.
     * @param podName Name of the pod.
     * @return V1ContainerStateTerminated.
     */
    @SneakyThrows({ApiException.class, IOException.class})
    private Optional<V1ContainerStateTerminated> createAWatchAndReturnOnTermination(String namespace, String podName) {
        log.debug("Creating a watch for pod {}/{}", namespace, podName);
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1ServiceAccount> callback = new K8AsyncCallback<>("createAWatchAndReturnOnTermination-" + podName);
        @Cleanup
        Watch<V1Pod> watch = Watch.createWatch(
                client,
                api.listNamespacedPodCall(namespace, PRETTY_PRINT, ALLOW_WATCH_BOOKMARKS, null, null, "POD_NAME=" + podName, null,
                        null, null, null, Boolean.TRUE, callback),
                new TypeToken<Watch.Response<V1Pod>>() {
                }.getType());

        for (Watch.Response<V1Pod> v1PodResponse : watch) {

            List<V1ContainerStatus> containerStatuses = v1PodResponse.object.getStatus().getContainerStatuses();
            log.debug("Container status for the pod {} is {}", podName, containerStatuses);
            if (containerStatuses == null || containerStatuses.size() == 0) {
                log.debug("Container status is not part of the pod {}/{}, wait for the next update from KUBERNETES Cluster", namespace, podName);
                continue;
            }
            // We check only the first container as there is only one container in the pod.
            V1ContainerState containerStatus = containerStatuses.get(0).getState();
            log.debug("Current container status is {}", containerStatus);
            if (containerStatus.getTerminated() != null) {
                log.info("Pod {}/{} has terminated", namespace, podName);
                return Optional.of(containerStatus.getTerminated());
            }
        }
        return Optional.empty();
    }

    /**
     * Method to get V1ConfigMap.
     * @param name Name of the ConfigMap.
     * @param namespace Namespace on which the pod(s) reside.
     * @return Future representing the V1ConfigMap.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1ConfigMap> getConfigMap(String name, String namespace) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1ConfigMap> callback = new K8AsyncCallback<>("readNamespacedConfigMap-" + name);
        api.readNamespacedConfigMapAsync(name, namespace, PRETTY_PRINT,  callback);
        return callback.getFuture();
    }

    /**
     * Create ConfigMap.
     * @param namespace The namespace where the ConfigMap should be created.
     * @param binding The cluster ConfigMap.
     * @return A future indicating the status of the ConfigMap operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1ConfigMap> createConfigMap(String namespace, V1ConfigMap binding) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1ConfigMap> callback = new K8AsyncCallback<>("createConfigMap-" + binding.getMetadata().getName());
        api.createNamespacedConfigMapAsync(namespace, binding, PRETTY_PRINT, DRY_RUN, FIELD_MANAGER, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Method to delete V1ConfigMap.
     * @param name Name of the ConfigMap.
     * @param namespace Namespace on which the pod(s) reside.
     * @return Future representing the V1ConfigMap.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Status> deleteConfigMap(String name, String namespace) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1Status> callback = new K8AsyncCallback<>("deleteNamespacedConfigMap-" + name);
        api.deleteNamespacedConfigMapAsync(name, namespace, PRETTY_PRINT, null, 0, false, null, null, callback);
        return callback.getFuture();
    }

    /**
     * Method to create V1Secret.
     * @param namespace Namespace in which the secret should be created. Secrets cannot be shared outside namespace.
     * @param secret V1Secret to create
     * @return Future representing the V1Secret.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Secret> createSecret(String namespace, V1Secret secret) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1Secret> callback = new K8AsyncCallback<>("createNamespacedSecret-" + secret.getMetadata().getName());
        api.createNamespacedSecretAsync(namespace, secret, PRETTY_PRINT, null, null, null, callback);
        return exceptionallyExpecting(callback.getFuture(), isConflict, null);
    }

    /**
     * Method to get V1Secret.
     * @param name Name of the Secret.
     * @param namespace Namespace on which the pod(s) reside.
     * @return Future representing the V1Secret.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Secret> getSecret(String name, String namespace) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1Secret> callback = new K8AsyncCallback<>("readNamespacedSecret-" + name);
        api.readNamespacedSecretAsync(name, namespace, PRETTY_PRINT, callback);
        return callback.getFuture();
    }

    /**
     * Method to delete V1Secret.
     * @param name Name of the Secret.
     * @param namespace Namespace on which the pod(s) reside.
     * @return Future representing the V1Secret.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Status> deleteSecret(String name, String namespace) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1Status> callback = new K8AsyncCallback<>("deleteNamespacedSecret-" + name);
        api.deleteNamespacedSecretAsync(name, namespace, PRETTY_PRINT, null, 0, false, null, null, callback);
        return callback.getFuture();
    }

    /**
     * A method which returns a completed future once the desired number of pod(s) are running with a given label.
     * @param namespace Namespace
     * @param labelName Label name.
     * @param labelValue Value of the Label.
     * @param expectedPodCount Number of pods that need to be running.
     * @return A future which completes once the number of running pods matches the given criteria.
     */
    public CompletableFuture<Void> waitUntilPodIsRunning(String namespace, String labelName, String labelValue, int expectedPodCount) {

        AtomicBoolean shouldRetry = new AtomicBoolean(true);

        return Futures.loop(shouldRetry::get,
                () -> Futures.delayedFuture(Duration.ofSeconds(5), executor) // wait for 5 seconds before checking for status.
                        .thenCompose(v -> getStatusOfPodWithLabel(namespace, labelName, labelValue)) // fetch status of pods with the given label.
                        .thenApply(podStatuses -> podStatuses.stream()
                                // check for pods where all containers are running.
                                .filter(podStatus -> {
                                    if (podStatus.getContainerStatuses() == null) {
                                        return false;
                                    } else {
                                        return podStatus.getContainerStatuses()
                                                .stream()
                                                .allMatch(st -> st.getState().getRunning() != null);
                                    }
                                }).count()),
                runCount -> { // Number of pods which are running
                    log.info("Expected running pod count of {}:{}, actual running pod count of {}:{}.", labelValue, expectedPodCount, labelValue, runCount);
                    if (runCount == expectedPodCount) {
                        shouldRetry.set(false);
                    }
                }, executor);
    }

    /**
     * Download logs of the specified pod.
     *
     * @param fromPod Pod logs to be copied.
     * @param toFile Destination file of the logs.
     * @return A Future which completes once the download operation completes.
     */
    public CompletableFuture<Void> downloadLogs(final V1Pod fromPod, final String toFile) {

        final AtomicInteger retryCount = new AtomicInteger(0);
        return Retry.withExpBackoff(LOG_DOWNLOAD_INIT_DELAY_MS, 10, LOG_DOWNLOAD_RETRY_COUNT, RETRY_MAX_DELAY_MS)
                .retryingOn(TestFrameworkException.class)
                .throwingOn(RuntimeException.class)
                .runInExecutor(() -> {
                    final String podName = fromPod.getMetadata().getName();
                    log.debug("Download logs from pod {}", podName);
                    try {
                        @Cleanup
                        InputStream logStream = logUtility.streamNamespacedPodLog(fromPod);
                        // On every retry this method attempts to download the complete pod logs from from K8s api-server. Due to the
                        // amount of logs for a pod and the K8s cluster configuration it can so happen that the K8s api-server can
                        // return truncated logs. Hence, every retry attempt does not overwrite the previously downloaded logs for
                        // the pod.
                        String logFile = toFile + "-" + retryCount.incrementAndGet() + ".log";
                        Files.copy(logStream, Paths.get(logFile));
                        log.info("Logs downloaded from pod {} to {}", podName, logFile);
                    } catch (ApiException | IOException e) {
                        log.warn("Retryable error while downloading logs from pod {}. Error message: {} ", podName, e.getMessage());
                        throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Error while downloading logs");
                    }
                }, executor);
    }

    /**
     * Close resources used by KUBERNETES client.
     */
    public void close() {
        log.debug("Shutting down executor used by K8sClient");
        ExecutorServiceHelpers.shutdown(executor);

    }

    private static class K8AsyncCallback<T> implements ApiCallback<T> {
        private final String method;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        K8AsyncCallback(String method) {
            this.method = method;
        }

        @Override
        public void onFailure(ApiException e, int responseCode, Map<String, List<String>> responseHeaders) {
            if (CONFLICT.getStatusCode() == responseCode || NOT_FOUND.getStatusCode() == responseCode) {
                log.warn("Exception observed for method {} with response code {}", method, responseCode);
                log.warn("ResponseBody: {}", e.getResponseBody());
            } else {
                log.error("Exception observed for method {} with response code {}", method, responseCode, e);
                log.error("ResponseBody: {}", e.getResponseBody());
            }
            future.completeExceptionally(e);
        }

        @Override
        public void onSuccess(T t, int i, Map<String, List<String>> map) {
            log.debug("Method {} completed successfully with response code {} and value {}", method, i, t);
            future.complete(t);
        }

        @Override
        public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
            // NOP.
        }

        @Override
        public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
            // NOP.
        }

        CompletableFuture<T> getFuture() {
            return future;
        }
    }
}
