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
import io.kubernetes.client.ApiException;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerBuilder;
import io.kubernetes.client.models.V1ContainerPortBuilder;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentBuilder;
import io.kubernetes.client.models.V1DeploymentSpecBuilder;
import io.kubernetes.client.models.V1EnvVarBuilder;
import io.kubernetes.client.models.V1EnvVarSourceBuilder;
import io.kubernetes.client.models.V1LabelSelectorBuilder;
import io.kubernetes.client.models.V1ObjectFieldSelectorBuilder;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1PodSpecBuilder;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1PodTemplateSpecBuilder;
import io.kubernetes.client.models.V1beta1ClusterRole;
import io.kubernetes.client.models.V1beta1ClusterRoleBinding;
import io.kubernetes.client.models.V1beta1ClusterRoleBindingBuilder;
import io.kubernetes.client.models.V1beta1ClusterRoleBuilder;
import io.kubernetes.client.models.V1beta1CustomResourceDefinition;
import io.kubernetes.client.models.V1beta1CustomResourceDefinitionBuilder;
import io.kubernetes.client.models.V1beta1CustomResourceDefinitionNamesBuilder;
import io.kubernetes.client.models.V1beta1CustomResourceDefinitionSpecBuilder;
import io.kubernetes.client.models.V1beta1PolicyRuleBuilder;
import io.kubernetes.client.models.V1beta1RoleRefBuilder;
import io.kubernetes.client.models.V1beta1SubjectBuilder;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.kubernetes.K8Client;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Manage Zookeeper service on k8 cluster.
 */
@Slf4j
public class ZookeeperServiceOnK8 implements Service {

    private static final String NAMESPACE = "default";
    private final static String DEFAULT_NS = NAMESPACE;
    private static final Integer MIN_READY_SECONDS = 10; // minimum duration the operator is up and running to be considered ready.
    private static final String CUSTOM_RESOURCE_GROUP = "zookeeper.pravega.io";
    private static final String CUSTOM_RESOURCE_VERSION = "v1beta1";
    private static final String CUSTOM_RESOURCE_PLURAL = "zookeeper-clusters";
    private static final String ID = "example";
    private static final String OPERATOR_ID = "zookeeper-operator";
    private final K8Client k8Client;

    public ZookeeperServiceOnK8() {
        k8Client = new K8Client();
    }

    @Override
    public void start(boolean wait) {
        /*
            1. Check if operator is running ->
                If not then start the operator and wait for it to run.
             2. Check if zookeeper is running
                -> if not then instruct the operator to start it.
             3. wait until zk is running based on
         */
        CompletableFuture<Void> r1 = k8Client.createCRD(getZKOperatorCRD())
                                             .thenCompose(v -> k8Client.createClusterRole(getClusterRole()))
                                             .thenCompose(v -> k8Client.createClusterRoleBinding(getClusterRoleBinding()))
                                             //deploy zk operator.
                                             .thenCompose(v -> k8Client.createDeployment(NAMESPACE, getDeployment()))
                                             // wait until zk operator is running, only one instance of operator is running.
                                             .thenCompose(v -> k8Client.waitUntilPodIsRunning(NAMESPACE, "name", OPERATOR_ID, 1))
                                             // request operator to deploy zookeeper nodes.
                                             .thenCompose(v -> k8Client.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP, CUSTOM_RESOURCE_VERSION, NAMESPACE, CUSTOM_RESOURCE_PLURAL, getZookeeperDeployment("example", 2)))
                                             // wait until all the zookeeper pods are deployed by the operator.
                                             .thenCompose(v -> k8Client.waitUntilPodIsRunning(NAMESPACE, "app", ID, 2));

        Futures.await(r1);
    }




    @Override
    public void stop() {
        /*
            Inform zk operator to stop all the zk instances.
         */

    }

    @Override
    public void clean() {

    }

    @Override
    public String getID() {
        return ID;
    }

    @Override
    public boolean isRunning() {
        //check status of zk service.
        return false;
    }

    @Override
    public List<URI> getServiceDetails() {
        //fetch the URI.
        return null;
    }

    @Override
    public CompletableFuture<Void> scaleService(int instanceCount) {
        //Update the instance count.
        return null;
    }


    public void verifyZkOperator() throws IOException, ApiException {
        CompletableFuture<V1PodStatus> r = k8Client.getStatusOfPod(DEFAULT_NS, "zookeeper-operator");
        Futures.await(r);
        log.error("ZK operator {}", r.join());
        if (!Futures.isSuccessful(r)) {

        }
    }

    private void testPodStatus() {
        //        CompletableFuture<List<V1PodStatus>> r2 = k8Client.getStatusOfPodWithLabel("default", "name3", "zookeeper-operator");
        //            CompletableFuture<V1PodStatus> r1 = k8Client.getStatusOfPodWithLabel("default", "app", "example");
        //            Futures.await(r1);

        //        CompletableFuture<Void> r3 = k8Client.waitUntilPodIsRunning("name", "zookeeper-operator", 1);
        //        Futures.await(r3);
        //        System.out.println("stop");
    }

    private V1beta1ClusterRoleBinding getClusterRoleBinding() {
        return new V1beta1ClusterRoleBindingBuilder().withKind("ClusterRoleBinding")
                                                     .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                                                     .withMetadata(new V1ObjectMetaBuilder()
                                                                           .withName("default-account-zookeeper-operator")
                                                                           .build())
                                                     .withSubjects(new V1beta1SubjectBuilder().withKind("ServiceAccount")
                                                                                              .withName(NAMESPACE)
                                                                                              .withNamespace(NAMESPACE)
                                                                                              .build())
                                                     .withRoleRef(new V1beta1RoleRefBuilder().withKind("ClusterRole")
                                                                                             .withName("zookeeper-operator")
                                                                                             .withApiGroup("rbac.authorization.k8s.io")
                                                                                             .build())
                                                     .build();
    }

    private V1beta1CustomResourceDefinition getZKOperatorCRD() {
        final V1beta1CustomResourceDefinition def = new V1beta1CustomResourceDefinitionBuilder()
                .withApiVersion("apiextensions.k8s.io/v1beta1")
                .withKind("CustomResourceDefinition")
                .withMetadata(new V1ObjectMetaBuilder().withName("zookeeper-clusters.zookeeper.pravega.io").build())
                .withSpec(new V1beta1CustomResourceDefinitionSpecBuilder()
                                  .withGroup(CUSTOM_RESOURCE_GROUP)
                                  .withNames(new V1beta1CustomResourceDefinitionNamesBuilder()
                                                     .withKind("ZookeeperCluster")
                                                     .withListKind("ZookeeperClusterList")
                                                     .withPlural(CUSTOM_RESOURCE_PLURAL)
                                                     .withSingular("zookeeper-cluster")
                                                     .withShortNames("zk")
                                                     .build())
                                  .withScope("Namespaced")
                                  .withVersion(CUSTOM_RESOURCE_VERSION)
                                  .build())
                .build();
        return def;

    }

    private V1beta1ClusterRole getClusterRole() {
        final V1beta1ClusterRole role = new V1beta1ClusterRoleBuilder()
                .withKind("ClusterRole")
                .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                .withMetadata(new V1ObjectMetaBuilder().withName("zookeeper-operator").build())
                .withRules(new V1beta1PolicyRuleBuilder().withApiGroups(CUSTOM_RESOURCE_GROUP)
                                                         .withResources("*")
                                                         .withVerbs("*")
                                                         .build(),
                           new V1beta1PolicyRuleBuilder().withApiGroups("")
                                                         .withResources("pods", "services", "endpoints", "persistentvolumeclaims", "events", "configmaps", "secrets")
                                                         .withVerbs("*")
                                                         .build(),
                           new V1beta1PolicyRuleBuilder().withApiGroups("apps")
                                                         .withResources("deployments", "daemonsets", "replicasets", "statefulsets")
                                                         .withVerbs("*")
                                                         .build(),
                           new V1beta1PolicyRuleBuilder().withApiGroups("policy")
                                                         .withResources("poddisruptionbudgets")
                                                         .withVerbs("*")
                                                         .build())
                .build();
        return role;
    }


    private V1Deployment getDeployment() {
        V1Container container = new V1ContainerBuilder().withName("zookeeper-operator")
                                                        .withImage("pravega/zookeeper-operator:latest")
                                                        .withPorts(new V1ContainerPortBuilder().withContainerPort(60000).build())
                                                        .withCommand("zookeeper-operator")
                                                        .withImagePullPolicy("Always")
                                                        .withEnv(new V1EnvVarBuilder().withName("WATCH_NAMESPACE")
                                                                                      .withValueFrom(new V1EnvVarSourceBuilder()
                                                                                                             .withFieldRef(new V1ObjectFieldSelectorBuilder()
                                                                                                                                   .withFieldPath("metadata.namespace")
                                                                                                                                   .build())
                                                                                                             .build())
                                                                                      .build(),
                                                                 new V1EnvVarBuilder().withName("OPERATOR_NAME")
                                                                                      .withValueFrom(new V1EnvVarSourceBuilder()
                                                                                                             .withFieldRef(new V1ObjectFieldSelectorBuilder()
                                                                                                                                   .withFieldPath("metadata.name")
                                                                                                                                   .build())
                                                                                                             .build())
                                                                                      .build())
                                                        .build();
        return new V1DeploymentBuilder().withMetadata(new V1ObjectMetaBuilder().withName("zookeeper-operator")
                                                                               .withNamespace(NAMESPACE)
                                                                               .build())
                                        .withKind("Deployment")
                                        .withApiVersion("apps/v1")
                                        .withSpec(new V1DeploymentSpecBuilder().withMinReadySeconds(MIN_READY_SECONDS)
                                                                               .withSelector(new V1LabelSelectorBuilder()
                                                                                                     .withMatchLabels(ImmutableMap.of("name", "zookeeper-operator"))
                                                                                                     .build())
                                                                               .withTemplate(new V1PodTemplateSpecBuilder()
                                                                                                     .withMetadata(new V1ObjectMetaBuilder()
                                                                                                                           .withLabels(ImmutableMap.of("name", "zookeeper-operator"))
                                                                                                                           .build())
                                                                                                     .withSpec(new V1PodSpecBuilder()
                                                                                                                       .withContainers(container)
                                                                                                                       .build())
                                                                                                     .build())
                                                                               .build())
                                        .build();
    }

    private Map<String, Object> getZookeeperDeployment(final String deploymentName, final int clusterSize) {
        return ImmutableMap.<String, Object>builder()
                .put("apiVersion", "zookeeper.pravega.io/v1beta1")
                .put("kind", "ZookeeperCluster")
                .put("metadata", ImmutableMap.of("name", deploymentName))
                .put("spec", ImmutableMap.of("size", clusterSize))
                .build();
    }

    public static void main(String[] args) throws Exception {
        ZookeeperServiceOnK8 zkService = new ZookeeperServiceOnK8();
        //            zkService.test();
        //            zkService.testStatus();
        zkService.start(true);
        //        zkService.verifyZkOperator();
    }
}
