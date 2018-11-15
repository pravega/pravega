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
import io.kubernetes.client.apis.ApiextensionsV1beta1Api;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.apis.CustomObjectsApi;
import io.kubernetes.client.apis.RbacAuthorizationV1beta1Api;
import io.kubernetes.client.models.V1ContainerState;
import io.kubernetes.client.models.V1ContainerStateTerminated;
import io.kubernetes.client.models.V1ContainerStatus;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1Namespace;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1beta1ClusterRole;
import io.kubernetes.client.models.V1beta1ClusterRoleBinding;
import io.kubernetes.client.models.V1beta1CustomResourceDefinition;
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

    private static final String PRETTY_PRINT = null;
    private static final int DEFAULT_TIMEOUT_SECONDS = 60; // timeout of http client.
    private static final int RETRY_MAX_DELAY_MS = 10_000; // max time between retries to check if pod has completed.
    private static final int RETRY_COUNT = 50; // Max duration of a pod is 1 hour.
    private final ApiClient client;
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "pravega-k8-client");
    private final Retry.RetryWithBackoff retryWithBackoff = Retry.withExpBackoff(1000, 10, RETRY_COUNT, RETRY_MAX_DELAY_MS);

    public K8Client() {
        try {
            this.client = Config.defaultClient();
            this.client.setDebugging(false); // this can be set to true enable http dump.
            this.client.getHttpClient().setReadTimeout(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Configuration.setDefaultApiClient(this.client);
        } catch (IOException e) {
            throw new TestFrameworkException(ConnectionFailed, "Connection to the k8 cluster failed, ensure .kube/config is configured correctly.", e);
        }
    }

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

    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Namespace> createNameSpace(final String namespace) {
        // Namespace create operation on an already existing namespace causes K8 to stop responding, use the blocking version of api.
        V1Namespace body = new V1Namespace();

        // Set the required api version and kind of resource
        body.setApiVersion("v1");
        body.setKind("Namespace");

        // Setup the standard object metadata
        V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(namespace);
        body.setMetadata(meta);

        CoreV1Api api = new CoreV1Api();
        K8AsyncCallback<V1Namespace> callback = new K8AsyncCallback<>("createNamespace");

        api.createNamespaceAsync(body, PRETTY_PRINT, callback);
        return callback.getFuture()
                       .handle((r, t) -> {
                           if (t == null) {
                               return r;
                           } else {
                               if (t instanceof ApiException) {
                                   ApiException e = (ApiException) t;
                                   if (e.getCode() == Response.Status.CONFLICT.getStatusCode()) {
                                       log.info("Namespace {} already present, ignoring the exception.", namespace);
                                       return null;
                                   }
                               }
                               throw new CompletionException(t);
                           }
                       });
    }

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

    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Deployment> createDeployment(final String namespace, final V1Deployment deploy) {
        AppsV1Api api = new AppsV1Api();
        K8AsyncCallback<V1Deployment> callback = new K8AsyncCallback<>("deployment");
        api.createNamespacedDeploymentAsync(namespace, deploy, PRETTY_PRINT, callback );

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

    @SneakyThrows(ApiException.class)
    public CompletableFuture<V1Deployment> getDeploymentStatus(final String deploymentName, final String namespace) {
        AppsV1Api api = new AppsV1Api();
        K8AsyncCallback<V1Deployment> callback = new K8AsyncCallback<>("readNamespacedDeployment");
        api.readNamespacedDeploymentStatusAsync(deploymentName, namespace, PRETTY_PRINT, callback);

        return callback.getFuture();
    }


    @SneakyThrows(ApiException.class)
    public CompletableFuture<Object> createCustomObject(String customResourceGroup, String version, String namespace,
                                                         String plural, Map<String, Object> request) {
        CustomObjectsApi api = new CustomObjectsApi();
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("createCustomObject");
        api.createNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, request, PRETTY_PRINT, callback);
        return callback.getFuture();
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows(ApiException.class)
    public CompletableFuture<Object> createAndUpdateCustomObject(String customResourceGroup, String version, String namespace,
                                                        String plural, Map<String, Object> request) {
        CustomObjectsApi api = new CustomObjectsApi();

        //Fetch the name of the custom object.
        String name = ((Map<String, String>) request.get("metadata")).get("name");
        K8AsyncCallback<Object> callback = new K8AsyncCallback<>("getCustomObject");
        api.getNamespacedCustomObjectAsync(customResourceGroup, version, namespace, plural, name, callback);

        return callback.getFuture().handle((o, t) -> {
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
        CompletableFuture<Void> r = retryConfig.runAsync(() -> CompletableFuture.supplyAsync(() -> {
            Optional<V1ContainerStateTerminated> state = createAWatchAndReturnOnTermination(namespace, podName);
            if (state.isPresent()) {
                future.complete(state.get());
            } else {
                throw new RuntimeException("Watch did not return terminated state for pod " + podName);
            }
            return null;
        }), executor);
        return future;

    }

    @SneakyThrows({ApiException.class, IOException.class})
    private Optional<V1ContainerStateTerminated> createAWatchAndReturnOnTermination(String namespace, String podName) {
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
            if (containerStatuses.size() == 0) {
                log.error("Invalid State, atleast 1 container should be part of the pod {}/{}", namespace, podName);
                throw new IllegalStateException("Invalid pod state " + podName);
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

    public CompletableFuture<Void> waitUntilPodIsRunning(String namespace, String labelName, String labelValue, int atleastNumberOfPods) {

        AtomicBoolean shouldRetry = new AtomicBoolean(true);

        return Futures.loop(shouldRetry::get,
                            () -> Futures.delayedFuture(Duration.ofSeconds(5), executor) // wait for 5 seconds before checking for status.
                                         .thenCompose(v -> getStatusOfPodWithLabel(namespace, labelName, labelValue)) // fetch status of pods with the given label.
                                         .thenApply(podStatuses -> podStatuses.stream()
                                                                              // check for pods where all containers are running.
                                                                              .filter(podStatus -> podStatus.getContainerStatuses()
                                                                                                            .stream()
                                                                                                            .allMatch(st -> st.getState().getRunning() != null))
                                                                              .count()),
                            runCount -> { // Number of pods which are running
                                if (runCount >= atleastNumberOfPods) {
                                    shouldRetry.set(false);
                                }
                            }, executor);

    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(executor);

    }

    private static class K8AsyncCallback<T> implements ApiCallback<T> {
        private final String method;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        public K8AsyncCallback(String method) {
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
            log.debug("UploadProgress callback invoked bytesWritten:{} for contentLength: {} done: {}", bytesWritten,
                      contentLength, done);
        }

        @Override
        public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
            log.debug("DownloadProgress callback invoked, bytesRead: {} for contentLength: {} done: {}", bytesRead,
                      contentLength, done);
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }
    }


    public static void main(String[] args) throws IOException, ApiException {
        K8Client client = new K8Client();

        String namespace = "test123";
        client.createNamespace(namespace);

        //        String testPodName = "test2";
        //        CoreV1Api api = new CoreV1Api();
        //        V1Pod pod = new V1PodBuilder()
        //                .withNewMetadata().withName(testPodName).withLabels(ImmutableMap.of("POD_NAME", testPodName)).endMetadata()
        //                .withNewSpec().addNewContainer()
        //                .withName(testPodName)
        //                .withImage("openjdk:8-jre-alpine")
        //                .withImagePullPolicy("IfNotPresent")
        ////                .withCommand("/bin/sh", "-c", "wget http://asdrepo.isus.emc.com:8081/artifactory/nautilus-pravega-testframework/pravega/systemtests/maven-metadata.xml",
        ////                             "java -version",
        ////                             "/bin/sh"
        ////                )
        //                .withCommand("/bin/sh")
        //                .withArgs("-c", "sleep 60;wget http://asdrepo.isus.emc.com:8081/artifactory/nautilus-pravega-testframework/pravega/systemtests/maven-metadata.xml;java -version;" +
        //                        "echo ./maven-metadata.xml")
        //                .endContainer()
        //                .withRestartPolicy("Never")
        //                .endSpec().build();
        //
        //        log.info("Create a Pod");
        //        CompletableFuture<V1Pod> pod1 = client.deployPod(namespace, pod);
        //        Futures.await(pod1);
        //        client.waitUntilPodCompletes(namespace, testPodName).join();
        //
        //        Futures.await(client.getStatusOfPod(namespace, testPodName));

    }
}
