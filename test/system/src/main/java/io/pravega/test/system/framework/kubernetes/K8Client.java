/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.kubernetes;

import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.ApiCallback;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.apis.ApiextensionsV1beta1Api;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.apis.CustomObjectsApi;
import io.kubernetes.client.apis.RbacAuthorizationV1beta1Api;
import io.kubernetes.client.models.V1ContainerState;
import io.kubernetes.client.models.V1ContainerStateTerminated;
import io.kubernetes.client.models.V1ContainerStatus;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1Namespace;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1beta1ClusterRole;
import io.kubernetes.client.models.V1beta1ClusterRoleBinding;
import io.kubernetes.client.models.V1beta1CustomResourceDefinition;
import io.kubernetes.client.models.V1beta1Role;
import io.kubernetes.client.models.V1beta1RoleBinding;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.TestFrameworkException;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.pravega.test.system.framework.TestFrameworkException.Type.ConnectionFailed;

@Slf4j
public class K8Client implements AutoCloseable {

    private static final int DEFAULT_TIMEOUT_SECONDS = 60; // timeout of http client.
    private static final int RETRY_MAX_DELAY_MS = 10_000; // max time between retries to check if pod has completed.
    private static final int RETRY_COUNT = 50; // Max duration of a pod is 1 hour.
    private static final String PRETTY_PRINT = null;
    private final ApiClient client;
    private final PodLogs logUtility;
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "pravega-k8-client");
    private final Retry.RetryWithBackoff retryWithBackoff = Retry.withExpBackoff(1000, 10, RETRY_COUNT, RETRY_MAX_DELAY_MS);

    /**
     * Create an instance of K8Client. The k8 config used follows the below pattern.
     *      1. If $KUBECONFIG is defined, use that config file.
     *      2. If $HOME/.kube/config can be found, use that.
     *      3. If the in-cluster service account can be found, assume in cluster config.
     *      4.Default to localhost:8080 as a last resort.
     */
    public K8Client() {
        try {
            this.client = Config.defaultClient();
            this.client.setDebugging(false); // this can be set to true enable http dump.
            this.client.getHttpClient().setReadTimeout(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Configuration.setDefaultApiClient(this.client);
            this.logUtility = new PodLogs();
        } catch (IOException e) {
            throw new TestFrameworkException(ConnectionFailed, "Connection to the k8 cluster failed, ensure .kube/config is configured correctly.", e);
        }
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
            V1Namespace existing = api.readNamespace(namespace, PRETTY_PRINT, Boolean.FALSE, Boolean.FALSE);
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

        return api.createNamespace(body, PRETTY_PRINT);
    }

    /**
     * Deploy a pod. This ignores exception incase the pod has already been deployed.
     * @param namespace Namespace.
     * @param pod Pod details.
     * @return Future which is completed once the deployemnt has been triggered.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Pod> deployPod(final String namespace, final V1Pod pod) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1Pod> callback = new K8AsyncCallback<>("createPod");
        api.createNamespacedPodAsync(namespace, pod, PRETTY_PRINT, callback);

        return callback.getFuture()
                       .handle((r, t) -> {
                           if (t == null) {
                               return r;
                           } else {
                               if (t instanceof ApiException) {
                                   ApiException e = (ApiException) t;
                                   if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                                       log.info("Pod {} is already running in namespace {}, ignoring the exception.",
                                                pod.getMetadata().getName(), namespace);
                                       return null;
                                   }
                               }
                               throw new CompletionException(t);
                           }
                       });
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
        api.listNamespacedPodAsync(namespace, PRETTY_PRINT, null, null, true, "POD_NAME=" + podName, null,
                                   null, null, false, callback);
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
    @SneakyThrows(ApiException.class)
    public CompletableFuture<List<V1PodStatus>> getStatusOfPodWithLabel(final String namespace, final String labelName, final String labelValue) {
        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1PodList> callback = new K8AsyncCallback<>("listPods");
        api.listNamespacedPodAsync(namespace, PRETTY_PRINT, null, null, true, labelName + "=" + labelValue, null,
                                   null, null, false, callback);
        return callback.getFuture()
                       .thenApply(v1PodList -> {
                           List<V1Pod> podList = v1PodList.getItems();
                           log.debug("{} pod(s) found with label {}={}.", podList.size(), labelName, labelValue);
                           return podList.stream().map(V1Pod::getStatus).collect(Collectors.toList());
                       });
    }

    /**
     * Create a deployement on K8, if the deployment is already present then it is ignored.
     * @param namespace Namespace.
     * @param deploy Deployment object.
     * @return A future which represents the creation of the Deployment.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Deployment> createDeployment(final String namespace, final V1Deployment deploy) {
        AppsV1Api api = new AppsV1Api();
        K8AsyncCallback<V1Deployment> callback = new K8AsyncCallback<>("deployment");
        api.createNamespacedDeploymentAsync(namespace, deploy, PRETTY_PRINT, callback);

        return callback.getFuture()
                       .handle((r, t) -> {
                           if (t == null) {
                               return r;
                           } else {
                               if (t instanceof ApiException) {
                                   ApiException e = (ApiException) t;
                                   if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                                       log.info("Deployment {} is already running in namespace {}, ignoring the exception.",
                                                deploy.getMetadata().getName(), namespace);
                                       return null;
                                   }
                               }
                               throw new CompletionException(t);
                           }
                       });
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
        K8AsyncCallback<V1Deployment> callback = new K8AsyncCallback<>("readNamespacedDeployment");
        api.readNamespacedDeploymentStatusAsync(deploymentName, namespace, PRETTY_PRINT, callback);

        return callback.getFuture();
    }


    /**
     * Create a Custom object for a CRD. This is useful while interacting with operators.
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
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("createCustomObject");
        api.createNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, request, PRETTY_PRINT, callback);
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
        CompletableFuture<Object> f = getCustomObject(customResourceGroup, version, namespace, plural, name);

        return f.handle((o, t) -> {
            CompletableFuture<Object> future = null;
            if (t != null) {
                log.warn("Exception while trying to fetch instance {} of custom resource {}, try to create it.", name, customResourceGroup, t);
                try {
                    //create object
                    K8AsyncCallback<Object> cb = new K8AsyncCallback<>("createCustomObject");
                    api.createNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, request, PRETTY_PRINT, cb);
                    future = cb.getFuture();
                } catch (ApiException e) {
                    throw new CompletionException(e);
                }
            }
            if (o != null) {
                log.info("Instance {} of custom resource {}  exists, update it with the new request", name, customResourceGroup);
                try {
                    //patch object
                    K8AsyncCallback<Object> cb = new K8AsyncCallback<>("patchCustomObject");
                    api.patchNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, name, request, cb);
                    future = cb.getFuture();
                } catch (ApiException e) {
                    throw new CompletionException(e);
                }
            }

            return future;
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
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("getCustomObject");
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
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("getCustomObject");
        api.deleteNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, name, options,
                                              0, false, null, callback);

        return callback.getFuture();
    }

    /**
     * Create a CRD.
     * @param crd Custom resource defnition.
     * @return A future indicating the status of this operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1beta1CustomResourceDefinition> createCRD(final V1beta1CustomResourceDefinition crd) {
        ApiextensionsV1beta1Api api = new ApiextensionsV1beta1Api();
        K8AsyncCallback<V1beta1CustomResourceDefinition> callback = new K8AsyncCallback<>("create CRD");
        api.createCustomResourceDefinitionAsync(crd, PRETTY_PRINT, callback);

        return callback.getFuture().handle((r, ex) -> {
            if (ex == null) {
                return r;
            } else {
                if (ex instanceof ApiException) {
                    ApiException e = (ApiException) ex;
                    if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                        log.info("CRD {} is already created, ignoring the exception.", crd.getMetadata().getName());
                        return null;
                    }
                }
                throw new CompletionException(ex);
            }
        });
    }

    /**
     * Create a cluster role.
     * @param role Cluster Role.
     * @return A future indicating the status of this operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1beta1ClusterRole> createClusterRole(V1beta1ClusterRole role) {
        RbacAuthorizationV1beta1Api api = new RbacAuthorizationV1beta1Api();
        K8AsyncCallback<V1beta1ClusterRole> callback = new K8AsyncCallback<>("createClusterRole");
        api.createClusterRoleAsync(role, PRETTY_PRINT, callback);

        return callback.getFuture().handle((r, ex) -> {
            if (ex == null) {
                return r;
            } else {
                if (ex instanceof ApiException) {
                    ApiException e = (ApiException) ex;
                    if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                        log.info("ClusterRole {} is already created, ignoring the exception.", role.getMetadata().getName());
                        return null;
                    }
                }
                throw new CompletionException(ex);
            }
        });
    }

    /**
     * Create a role.
     * @param namespace Namespace where the role is created.
     * @param role Role.
     * @return A future indicating the status of this operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1beta1Role> createRole(String namespace, V1beta1Role role) {
        RbacAuthorizationV1beta1Api api = new RbacAuthorizationV1beta1Api();
        K8AsyncCallback<V1beta1Role> callback = new K8AsyncCallback<>("createRole");
        api.createNamespacedRoleAsync(namespace, role, PRETTY_PRINT, callback);

        return callback.getFuture().handle((r, ex) -> {
            if (ex == null) {
                return r;
            } else {
                if (ex instanceof ApiException) {
                    ApiException e = (ApiException) ex;
                    if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                        log.info("Role {} is already created, ignoring the exception.", role.getMetadata().getName());
                        return null;
                    }
                }
                throw new CompletionException(ex);
            }
        });
    }

    /**
     * Create cluster role binding.
     * @param binding The cluster role binding.
     * @return A future indicating the status of the create role binding operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1beta1ClusterRoleBinding> createClusterRoleBinding(V1beta1ClusterRoleBinding binding) {
        RbacAuthorizationV1beta1Api api = new RbacAuthorizationV1beta1Api();
        K8AsyncCallback<V1beta1ClusterRoleBinding> callback = new K8AsyncCallback<>("createClusterRoleBinding");
        api.createClusterRoleBindingAsync(binding, PRETTY_PRINT, callback);
        return callback.getFuture().handle((r, ex) -> {
            if (ex == null) {
                return r;
            } else {
                if (ex instanceof ApiException) {
                    ApiException e = (ApiException) ex;
                    if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                        log.info("ClusterRoleBinding {} is already present, ignoring the exception.", binding.getMetadata().getName());
                        return null;
                    }
                }
                throw new CompletionException(ex);
            }
        });
    }

    /**
     * Create role binding.
     * @param namespace The namespece where the binding should be created.
     * @param binding The cluster role binding.
     * @return A future indicating the status of the create role binding operation.
     */
    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1beta1RoleBinding> createRoleBinding(String namespace, V1beta1RoleBinding binding) {
        RbacAuthorizationV1beta1Api api = new RbacAuthorizationV1beta1Api();
        K8AsyncCallback<V1beta1RoleBinding> callback = new K8AsyncCallback<>("createRoleBinding");
        api.createNamespacedRoleBindingAsync(namespace, binding, PRETTY_PRINT, callback);
        return callback.getFuture().handle((r, ex) -> {
            if (ex == null) {
                return r;
            } else {
                if (ex instanceof ApiException) {
                    ApiException e = (ApiException) ex;
                    if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                        log.info("Role binding {} is already present, ignoring the exception.", binding.getMetadata().getName());
                        return null;
                    }
                }
                throw new CompletionException(ex);
            }
        });
    }

    /**
     * A method which returns a completed future once a given Pod has completed execution. This is useful to track test execution.
     * This method uses a Watch to track the status of the pod. The maximum wait time is based on the retry configuration.
     * @param namespace Namespace.
     * @param podName Pod name.
     * @return A future which is complete once the pod completes.
     */
    public CompletableFuture<V1ContainerStateTerminated> waitUntilPodCompletes(final String namespace, final String podName) {

        CompletableFuture<V1ContainerStateTerminated> future = new CompletableFuture<>();

        Retry.RetryAndThrowConditionally retryConfig = retryWithBackoff
                .retryWhen(t -> {
                    Throwable ex = Exceptions.unwrap(t);
                    if (ex.getCause() instanceof IOException) {
                        log.warn("Exception while fetching status of pod, will attempt a retry", ex.getCause());
                        return true;
                    }
                    log.error("Exception while fetching status of pod", ex);
                    return false;
                });

        retryConfig.runInExecutor(() -> {
            Optional<V1ContainerStateTerminated> state = createAWatchAndReturnOnTermination(namespace, podName);
            if (state.isPresent()) {
                future.complete(state.get());
            } else {
                throw new RuntimeException("Watch did not return terminated state for pod " + podName);
            }
        }, executor);

        return future;
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
        @Cleanup
        Watch<V1Pod> watch = Watch.createWatch(
                client,
                api.listNamespacedPodCall(namespace, PRETTY_PRINT, null, null, true, "POD_NAME=" + podName, null,
                                          null, null, Boolean.TRUE, null, null),
                new TypeToken<Watch.Response<V1Pod>>() {
                }.getType());

        for (Watch.Response<V1Pod> v1PodResponse : watch) {

            List<V1ContainerStatus> containerStatuses = v1PodResponse.object.getStatus().getContainerStatuses();
            log.debug("Container status for the pod {} is {}", podName, containerStatuses);
            if (containerStatuses == null || containerStatuses.size() == 0) {
                log.debug("Container status is not part of the pod {}/{}, wait for the next update from K8 Cluster", namespace, podName);
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
     * A method which returns a completed future once the pod is running for a given label.
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
                                log.debug("Expected running pod count : {}, actual running pod count :{}.", expectedPodCount, runCount);
                                if (runCount == expectedPodCount) {
                                    shouldRetry.set(false);
                                }
                            }, executor);
    }

    /**
     * Save logs of the specified pod.
     * @param fromPod Pod logs to be copied.
     * @param toFile destination file of the logs.
     */
    public void saveLogs(final V1Pod fromPod, final String toFile) {
        log.debug("copy logs from pod {} to file {}", fromPod.getMetadata().getName(), toFile);
        try {
            @Cleanup
            InputStream r = logUtility.streamNamespacedPodLog(fromPod);
            Files.copy(r, Paths.get(toFile));
        } catch (ApiException | IOException e) {
            log.error("Error while copying files from pod {}.", fromPod.getMetadata().getName());
        }

    }

    @Override
    public void close() {
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
            log.error("Exception observed for method {} with response code {}", method, responseCode, e);
            future.completeExceptionally(e);
        }

        @Override
        public void onSuccess(T t, int i, Map<String, List<String>> map) {
            log.info("Method {} completed successfully with response code {} and value {}", method, i, t);
            future.complete(t);
        }

        @Override
        public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
            log.trace("UploadProgress callback invoked bytesWritten:{} for contentLength: {} done: {}", bytesWritten,
                      contentLength, done);
        }

        @Override
        public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
            log.trace("DownloadProgress callback invoked, bytesRead: {} for contentLength: {} done: {}", bytesRead,
                      contentLength, done);
        }

        CompletableFuture<T> getFuture() {
            return future;
        }
    }
}
