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

import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodBuilder;
import io.kubernetes.client.models.V1ServiceAccount;
import io.kubernetes.client.models.V1ServiceAccountBuilder;
import io.kubernetes.client.models.V1beta1ClusterRoleBinding;
import io.kubernetes.client.models.V1beta1ClusterRoleBindingBuilder;
import io.kubernetes.client.models.V1beta1RoleRefBuilder;
import io.kubernetes.client.models.V1beta1SubjectBuilder;
import io.pravega.test.system.framework.TestExecutor;
import io.pravega.test.system.framework.kubernetes.ClientFactory;
import io.pravega.test.system.framework.kubernetes.K8sClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

@Slf4j
public class K8SequentialExecutor implements TestExecutor {

    private static final String NAMESPACE = "default"; // K8s namespace where the tests run.
    private static final String SERVICE_ACCOUNT = "test-framework"; //Service Account used by the test pod.

    @Override
    public CompletableFuture<Void> startTestExecution(Method testMethod) {
        final String className = testMethod.getDeclaringClass().getName();
        final String methodName = testMethod.getName();
        // pod name is the combination of a test method name and random Alphanumeric. It cannot be more than 63 characters.
        final String podName = (methodName + "-" + randomAlphanumeric(5)).toLowerCase();
        log.info("Start execution of test {}#{} on the K8s Cluster", className, methodName);

        final K8sClient client = ClientFactory.INSTANCE.getK8sClient();
        final V1Pod pod = getTestPod(className, methodName, podName.toLowerCase());

        return client.createServiceAccount(NAMESPACE, getServiceAccount()) // create service Account, ignore if already present.
                     .thenCompose(v1 -> client.createClusterRoleBinding(getClusterRoleBinding())) // ensure test pod has cluster admin rights.
                     .thenCompose(v -> client.deployPod(NAMESPACE, pod)) // deploy test pod.
                     .thenCompose(v -> client.waitUntilPodCompletes(NAMESPACE, podName))
                     .handle((s, t) -> {
                         if (t == null) {
                             log.info("Test execution completed with status {}", s);
                             client.saveLogs(pod, "./build/test-results/" + podName + ".log"); //save test log.
                             if (s.getExitCode() != 0) {
                                 log.error("Test {}#{} failed. Details: {}", className, methodName, s);
                                 throw new AssertionError(methodName + " test failed.");
                             } else {
                                 return null;
                             }

                         } else {
                             throw new CompletionException("Error while invoking the test " + podName, t);
                         }
                     });
    }

    private V1Pod getTestPod(String className, String methodName, String podName) {
        String repoUrl = System.getProperty("repoUrl");
        String testVersion = System.getProperty("testVersion");
        return new V1PodBuilder()
                .withNewMetadata().withName(podName).withNamespace(NAMESPACE).withLabels(ImmutableMap.of("POD_NAME", podName)).endMetadata()
                .withNewSpec().withServiceAccountName(SERVICE_ACCOUNT).withAutomountServiceAccountToken(true)
                .addNewContainer()
                .withName(podName) // container name is same as that of the pod.
                .withImage("openjdk:8-jre-alpine")
                .withImagePullPolicy("IfNotPresent")
                .withCommand("/bin/sh")
                .withArgs("-c",
                          "wget " + repoUrl + "/io/pravega/pravega-test-system/" + testVersion + "/pravega-test-system-" + testVersion +".jar && "
                                  + "echo \"download of system test jar complete\" && "
                                  + "java -DexecType=K8s -cp ./pravega-test-system-" + testVersion + ".jar io.pravega.test.system.SingleJUnitTestRunner "
                                  + className + "#" +methodName /*+ " > server.log 2>&1 */ + "; exit $?")
                .endContainer()
                .withRestartPolicy("Never")
                .endSpec().build();
    }

    @Override
    public void stopTestExecution() {
        throw new NotImplementedException("Not implemented for Kubernetes based tests");
    }

    private V1ServiceAccount getServiceAccount() {
        return new V1ServiceAccountBuilder()
                .withApiVersion("v1")
                .withKind("ServiceAccount")
                .withMetadata(new V1ObjectMetaBuilder()
                                      .withNamespace(NAMESPACE)
                                      .withName(SERVICE_ACCOUNT)
                                      .build())

                .build();
    }

    private V1beta1ClusterRoleBinding getClusterRoleBinding() {
        return new V1beta1ClusterRoleBindingBuilder().withKind("ClusterRoleBinding")
                                                     .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                                                     .withMetadata(new V1ObjectMetaBuilder()
                                                                           .withName("cluster-admin-testFramework")
                                                                           .withNamespace(NAMESPACE)
                                                                           .build())
                                                     .withSubjects(new V1beta1SubjectBuilder().withKind("ServiceAccount")
                                                                                              .withName(SERVICE_ACCOUNT)
                                                                                              .withNamespace(NAMESPACE)
                                                                                              .build())
                                                     .withRoleRef(new V1beta1RoleRefBuilder().withKind("ClusterRole")
                                                                                             .withName("cluster-admin")
                                                                                             .withApiGroup("") // all core apis.
                                                                                             .build()).build();
    }
}
