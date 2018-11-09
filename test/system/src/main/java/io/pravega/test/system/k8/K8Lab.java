/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.k8;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Namespace;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodBuilder;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1Status;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import io.swagger.annotations.Api;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Experiments with the k8 client.
 */
@Slf4j
public class K8Lab {
    public static void main(String[] args) throws ApiException, IOException {
        ApiClient client = initK8Client();

        List<Integer> r = new ArrayList<>();
        listAllPods();
        System.out.println("=================");
        //deploy();
        //watchNamespaces(client);

        CompletableFuture<Void> r1 = CompletableFuture.supplyAsync(() -> {
            try {
                return watchPods(client);
            } catch (ApiException e) {
                e.printStackTrace();
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });
        deletePod("test1", "default");
        deployTestContainer("test1");
        r1.join();
    }

    private static void deployTestContainer(String testPodName) throws ApiException {
        CoreV1Api api = new CoreV1Api();
        V1Pod pod = new V1PodBuilder()
                .withNewMetadata().withName(testPodName).endMetadata()
                .withNewSpec().addNewContainer()
                .withName(testPodName)
                .withImage("openjdk:8-jre-alpine")
                .withImagePullPolicy("IfNotPresent")
//                .withCommand("/bin/sh", "-c", "wget http://asdrepo.isus.emc.com:8081/artifactory/nautilus-pravega-testframework/pravega/systemtests/maven-metadata.xml",
//                             "java -version",
//                             "/bin/sh"
//                )
                .withCommand("/bin/sh")
                .withArgs("-c", "wget http://asdrepo.isus.emc.com:8081/artifactory/nautilus-pravega-testframework/pravega/systemtests/maven-metadata.xml;java -version;" +
                        "echo ./maven-metadata.xml")
                .endContainer()
                .withRestartPolicy("Never")
                .endSpec().build();

        api.createNamespacedPod("default", pod, null);


        V1PodList list = api.listNamespacedPod("default", null, null, null, null, null, null, null, null, null);
        for (V1Pod item : list.getItems()) {
            System.out.println(item.getMetadata().getName());
        }

    }

    private static void deploy() throws ApiException {

        CoreV1Api api = new CoreV1Api();

        V1Pod pod =
                new V1PodBuilder()
                        .withNewMetadata()
                        .withName("apod")
                        .endMetadata()
                        .withNewSpec()
                        .addNewContainer()
                        .withName("www")
                        .withImage("nginx")
                        .endContainer()
                        .endSpec()
                        .build();

        api.createNamespacedPod("default", pod, null);

        V1Pod pod2 =
                new V1Pod()
                        .metadata(new V1ObjectMeta().name("anotherpod"))
                        .spec(
                                new V1PodSpec()
                                        .containers(Arrays.asList(new V1Container().name("www").image("nginx"))));

        api.createNamespacedPod("default", pod2, null);

        V1PodList list =
                api.listNamespacedPod("default", null, null, null, null, null, null, null, null, null);
        for (V1Pod item : list.getItems()) {
            System.out.println(item.getMetadata().getName());
        }
    }

    private static void deletePod(String podName, String namespace) throws ApiException {

        CoreV1Api api = new CoreV1Api();
        try {
            V1Status result = api.deleteNamespacedPod(podName, namespace, new V1DeleteOptions(), null, 0, null, null);
        } catch (ApiException e) {
            if (e.getCode() == (NOT_FOUND.getStatusCode())) {
                log.info("Pod : {} under namespace {} already deleted", podName, namespace);
            }
        } catch (JsonSyntaxException e) {
            if (e.getCause() instanceof IllegalStateException) {
                IllegalStateException ise = (IllegalStateException) e.getCause();
                if (ise.getMessage() != null && ise.getMessage().contains("Expected a string but was BEGIN_OBJECT"))
                    log.debug("Catching exception because of issue https://github.com/kubernetes-client/java/issues/86", e);
                else throw e;
            } else throw e;
        }
    }


    private static void watchNamespaces(ApiClient client) throws ApiException, IOException {
        CoreV1Api api = new CoreV1Api();
        @Cleanup
        Watch<V1Namespace> watch =
                Watch.createWatch(
                        client,
                        api.listNamespaceCall(
                                null, null, null, null, null, 5, null, null, Boolean.TRUE, null, null),
                        new TypeToken<Watch.Response<V1Namespace>>() {
                        }.getType());

        for (Watch.Response<V1Namespace> item : watch) {
            System.out.printf("%s : %s%n", item.type, item.object.getMetadata().getName());
        }
    }


    private static Void watchPods(ApiClient client) throws ApiException, IOException {
        CoreV1Api api = new CoreV1Api();
        @Cleanup
        Watch<V1Pod> watch = Watch.createWatch(
                client,
                api.listPodForAllNamespacesCall(null, null, null, null, null, null, null, null, Boolean.TRUE, null, null),
                new TypeToken<Watch.Response<V1Pod>>() {
                }.getType());
        for (Watch.Response<V1Pod> item : watch) {
            System.out.printf("===> %s : %s, %s%n ", item.type, item.object.getMetadata().getName(), item.object.getStatus());
        }
        return null;
    }

    private static void listAllPods() throws ApiException {
        CoreV1Api api = new CoreV1Api();
        V1PodList list = api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
        for (V1Pod item : list.getItems()) {
            System.out.println(item.getMetadata().getName());
        }
    }

    private static ApiClient initK8Client() throws IOException {
        ApiClient client = Config.defaultClient();
        client.getHttpClient().setReadTimeout(60, TimeUnit.SECONDS);
        Configuration.setDefaultApiClient(client);
        return client;
    }
}
