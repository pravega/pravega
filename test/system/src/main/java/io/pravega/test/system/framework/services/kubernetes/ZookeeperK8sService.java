/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services.kubernetes;

import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1ContainerPortBuilder;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentBuilder;
import io.kubernetes.client.openapi.models.V1DeploymentSpecBuilder;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1EnvVarSourceBuilder;
import io.kubernetes.client.openapi.models.V1LabelSelectorBuilder;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelectorBuilder;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1PodTemplateSpecBuilder;
import io.kubernetes.client.openapi.models.V1beta1ClusterRole;
import io.kubernetes.client.openapi.models.V1beta1ClusterRoleBinding;
import io.kubernetes.client.openapi.models.V1beta1ClusterRoleBindingBuilder;
import io.kubernetes.client.openapi.models.V1beta1ClusterRoleBuilder;
import io.kubernetes.client.openapi.models.V1beta1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1beta1CustomResourceDefinitionBuilder;
import io.kubernetes.client.openapi.models.V1beta1CustomResourceDefinitionNamesBuilder;
import io.kubernetes.client.openapi.models.V1beta1CustomResourceDefinitionSpecBuilder;
import io.kubernetes.client.openapi.models.V1beta1CustomResourceDefinitionStatus;
import io.kubernetes.client.openapi.models.V1beta1PolicyRuleBuilder;
import io.kubernetes.client.openapi.models.V1beta1RoleRefBuilder;
import io.kubernetes.client.openapi.models.V1beta1SubjectBuilder;
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
    private static final String CUSTOM_RESOURCE_PLURAL = "zookeeper-clusters";
    private static final String CUSTOM_RESOURCE_KIND = "ZookeeperCluster";
    private static final String OPERATOR_ID = "zookeeper-operator";
    private static final int DEFAULT_INSTANCE_COUNT = 1; // number of zk instances.
    private static final String ZOOKEEPER_IMAGE_NAME = System.getProperty("zookeeperImageName", "zookeeper");
    private static final String PRAVEGA_ZOOKEEPER_IMAGE_VERSION = System.getProperty("zookeeperImageVersion", "latest");

    public ZookeeperK8sService(String id) {
        super(id);
    }

    @Override
    public void start(boolean wait) {
        Futures.getAndHandleExceptions(k8sClient.createCRD(getZKOperatorCRD())
                                                .thenCompose(v -> k8sClient.createClusterRole(getClusterRole()))
                                                .thenCompose(v -> k8sClient.createClusterRoleBinding(getClusterRoleBinding()))
                                                // deploy zk operator.
                                                .thenCompose(v -> k8sClient.createDeployment(NAMESPACE, getDeployment()))
                                                // wait until zk operator is running, only one instance of operator is running.
                                                .thenCompose(v -> k8sClient.waitUntilPodIsRunning(NAMESPACE, "name", OPERATOR_ID, 1))
                                                // request operator to deploy zookeeper nodes.
                                                .thenCompose(v -> k8sClient.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP, CUSTOM_RESOURCE_VERSION,
                                                                                                        NAMESPACE, CUSTOM_RESOURCE_PLURAL,
                                                                                                        getZookeeperDeployment(getID(), DEFAULT_INSTANCE_COUNT))),
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
                                                      .count())
                        .thenApply(runCount -> runCount == DEFAULT_INSTANCE_COUNT)
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
                                                                                             .build()).build();
    }

    private V1beta1CustomResourceDefinition getZKOperatorCRD() {

        return new V1beta1CustomResourceDefinitionBuilder()
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
                                  .withNewSubresources()
                                  .withStatus(new V1beta1CustomResourceDefinitionStatus())
                                  .endSubresources()
                                  .build())
                .build();

    }

    private V1beta1ClusterRole getClusterRole() {
        return new V1beta1ClusterRoleBuilder()
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
    }


    private V1Deployment getDeployment() {
        V1Container container = new V1ContainerBuilder().withName("zookeeper-operator")
                                                        .withImage(ZOOKEEPER_OPERATOR_IMAGE)
                                                        .withPorts(new V1ContainerPortBuilder().withContainerPort(60000).build())
                                                        .withCommand("zookeeper-operator")
                                                        .withImagePullPolicy(IMAGE_PULL_POLICY)
                                                        .withEnv(new V1EnvVarBuilder().withName("WATCH_NAMESPACE")
                                                                                      .withValueFrom(new V1EnvVarSourceBuilder()
                                                                                                             .withFieldRef(new V1ObjectFieldSelectorBuilder()
                                                                                                                                   .withFieldPath("metadata.namespace")
                                                                                                                                   .build()).build()).build(),
                                                                 new V1EnvVarBuilder().withName("OPERATOR_NAME")
                                                                                      .withValueFrom(new V1EnvVarSourceBuilder()
                                                                                                             .withFieldRef(new V1ObjectFieldSelectorBuilder()
                                                                                                                                   .withFieldPath("metadata.name")
                                                                                                                                   .build()).build()).build())
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
                                                                                                                       .build()).build()).build())
                                        .build();
    }

    private Map<String, Object> getZookeeperDeployment(final String deploymentName, final int clusterSize) {
        return ImmutableMap.<String, Object>builder()
                .put("apiVersion", "zookeeper.pravega.io/v1beta1")
                .put("kind", CUSTOM_RESOURCE_KIND)
                .put("metadata", ImmutableMap.of("name", deploymentName))
                .put("spec", ImmutableMap.builder().put("image",  getImageSpec(DOCKER_REGISTRY + PREFIX + "/" + ZOOKEEPER_IMAGE_NAME, PRAVEGA_ZOOKEEPER_IMAGE_VERSION))
                                         .put("size", clusterSize)
                                         .build())
                .build();
    }
}
