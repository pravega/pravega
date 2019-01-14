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
import io.kubernetes.client.models.V1PodTemplateSpecBuilder;
import io.kubernetes.client.models.V1beta1CustomResourceDefinition;
import io.kubernetes.client.models.V1beta1CustomResourceDefinitionBuilder;
import io.kubernetes.client.models.V1beta1CustomResourceDefinitionNamesBuilder;
import io.kubernetes.client.models.V1beta1CustomResourceDefinitionSpecBuilder;
import io.kubernetes.client.models.V1beta1PolicyRuleBuilder;
import io.kubernetes.client.models.V1beta1Role;
import io.kubernetes.client.models.V1beta1RoleBinding;
import io.kubernetes.client.models.V1beta1RoleBindingBuilder;
import io.kubernetes.client.models.V1beta1RoleBuilder;
import io.kubernetes.client.models.V1beta1RoleRefBuilder;
import io.kubernetes.client.models.V1beta1SubjectBuilder;
import io.pravega.test.system.framework.kubernetes.ClientFactory;
import io.pravega.test.system.framework.kubernetes.K8sClient;
import io.pravega.test.system.framework.services.Service;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonList;

public abstract class AbstractService implements Service {

    public static final int CONTROLLER_GRPC_PORT = 9090;
    public static final int CONTROLLER_REST_PORT = 10080;
    protected static final String TCP = "tcp://";
    static final int DEFAULT_CONTROLLER_COUNT = 1;
    static final int DEFAULT_SEGMENTSTORE_COUNT = 1;
    static final int DEFAULT_BOOKIE_COUNT = 3;
    static final int MIN_READY_SECONDS = 10; // minimum duration the operator is up and running to be considered ready.
    static final int ZKPORT = 2181;

    static final int SEGMENTSTORE_PORT = 12345;
    static final int BOOKKEEPER_PORT = 3181;
    static final String NAMESPACE = "default";

    static final String PRAVEGA_OPERATOR = "pravega-operator";
    static final String CUSTOM_RESOURCE_GROUP_PRAVEGA = "pravega.pravega.io";
    static final String CUSTOM_RESOURCE_VERSION_PRAVEGA = "v1alpha1";
    static final String CUSTOM_RESOURCE_PLURAL_PRAVEGA = "pravegaclusters";
    static final String CUSTOM_RESOURCE_KIND_PRAVEGA = "PravegaCluster";
    static final String PRAVEGA_CONTROLLER_LABEL = "pravega-controller";
    static final String PRAVEGA_SEGMENTSTORE_LABEL = "pravega-segmentstore";
    static final String BOOKKEEPER_LABEL = "bookie";
    static final String PRAVEGA_ID = "pravega";
    static final String IMAGE_PULL_POLICY = System.getProperty("imagePullPolicy", "Always");
    private static final String DOCKER_REGISTRY =  System.getProperty("dockerRegistryUrl", "");
    private static final String PRAVEGA_VERSION = System.getProperty("imageVersion", "latest");
    private static final String PRAVEGA_BOOKKEEPER_VERSION = System.getProperty("pravegaBookkeeperVersion", PRAVEGA_VERSION);
    private static final String PRAVEGA_OPERATOR_VERSION = System.getProperty("pravegaOperatorVersion", "issue-116");
    private static final String PREFIX = System.getProperty("imagePrefix", "pravega");

    final K8sClient k8sClient;
    private final String id;

    AbstractService(final String id) {
        this.k8sClient = ClientFactory.INSTANCE.getK8sClient();
        this.id = id;
    }

    @Override
    public String getID() {
        return id;
    }

    CompletableFuture<Object> deployPravegaUsingOperator(final URI zkUri, int controllerCount, int segmentStoreCount, int bookieCount) {
        return k8sClient.createCRD(getPravegaCRD())
                        .thenCompose(v -> k8sClient.createRole(NAMESPACE, getPravegaOperatorRole()))
                        .thenCompose(v -> k8sClient.createRoleBinding(NAMESPACE, getPravegaOperatorRoleBinding()))
                        //deploy pravega operator.
                        .thenCompose(v -> k8sClient.createDeployment(NAMESPACE, getPravegaOperatorDeployment()))
                        // wait until pravega operator is running, only one instance of operator is running.
                        .thenCompose(v -> k8sClient.waitUntilPodIsRunning(NAMESPACE, "name", PRAVEGA_OPERATOR, 1))
                        // request operator to deploy zookeeper nodes.
                        .thenCompose(v -> k8sClient.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA, CUSTOM_RESOURCE_VERSION_PRAVEGA,
                                                                                NAMESPACE, CUSTOM_RESOURCE_PLURAL_PRAVEGA,
                                                                                getPravegaDeployment(zkUri.getAuthority(),
                                                                                                   controllerCount,
                                                                                                   segmentStoreCount,
                                                                                                   bookieCount)));
    }

    private Map<String, Object> getPravegaDeployment(String zkLocation, int controllerCount, int segmentStoreCount, int bookieCount) {

        // generate BookkeeperSpec.
        final Map<String, Object> bkPersistentVolumeSpec = getPersistentVolumeClaimSpec("10Gi", "standard");
        // use the latest version of bookkeeper.
        final Map<String, Object> bookeeperSpec = ImmutableMap.<String, Object>builder().put("image",
                                                                                             getImageSpec(DOCKER_REGISTRY + PREFIX + "/bookkeeper", PRAVEGA_BOOKKEEPER_VERSION))
                                                                                        .put("replicas", bookieCount)
                                                                                        .put("storage", ImmutableMap.builder()
                                                                                                                    .put("ledgerVolumeClaimTemplate", bkPersistentVolumeSpec)
                                                                                                                    .put("journalVolumeClaimTemplate", bkPersistentVolumeSpec)
                                                                                                                    .build())
                                                                                        .put("autoRecovery", true)
                                                                                        .build();

        // generate Pravega Spec.
        final Map<String, Object> pravegaPersistentVolumeSpec = getPersistentVolumeClaimSpec("20Gi", "standard");
        final ImmutableMap<String, String> options = ImmutableMap.<String, String>builder()
                // Segment store properties.
                .put("autoScale.muteInSeconds", "120")
                .put("autoScale.cooldownInSeconds", "120")
                .put("autoScale.cacheExpiryInSeconds", "120")
                .put("autoScale.cacheCleanUpInSeconds", "120")
                .put("curator-default-session-timeout", "10000")
                .put("bookkeeper.bkAckQuorumSize", "3")
                // Controller properties.
                .put("MAX_LEASE_VALUE", "60000")
                .put("RETENTION_FREQUENCY_MINUTES", "2")
                .put("log.level", "DEBUG")
                .build();
        final Map<String, Object> pravegaSpec = ImmutableMap.<String, Object>builder().put("controllerReplicas", controllerCount)
                                                                                      .put("segmentStoreReplicas", segmentStoreCount)
                                                                                      .put("debugLogging", true)
                                                                                      .put("cacheVolumeClaimTemplate", pravegaPersistentVolumeSpec)
                                                                                      .put("options", options)
                                                                                      .put("image",
                                                                                           getImageSpec(DOCKER_REGISTRY + PREFIX + "/pravega", PRAVEGA_VERSION))
                                                                                      .put("tier2", tier2Spec("pravega-tier2"))
                                                                                      .build();
        return ImmutableMap.<String, Object>builder()
                .put("apiVersion", "pravega.pravega.io/v1alpha1")
                .put("kind", CUSTOM_RESOURCE_KIND_PRAVEGA)
                .put("metadata", ImmutableMap.of("name", PRAVEGA_ID, "namespace", NAMESPACE))
                .put("spec", ImmutableMap.builder().put("zookeeperUri", zkLocation)
                                         .put("bookkeeper", bookeeperSpec)
                                         .put("pravega", pravegaSpec)
                                         .build())
                .build();
    }

    private Map<String, Object> getImageSpec(String imageName, String tag) {
        return ImmutableMap.<String, Object>builder().put("repository", imageName)
                                                     .put("tag", tag)
                                                     .put("pullPolicy", IMAGE_PULL_POLICY)
                                                     .build();
    }

    private Map<String, Object> tier2Spec(String tier2ClaimName) {
        return ImmutableMap.of("filesystem", ImmutableMap.of("persistentVolumeClaim",
                                                             ImmutableMap.of("claimName", tier2ClaimName)));
    }

    private Map<String, Object> getPersistentVolumeClaimSpec(String size, String storageClass) {
        return ImmutableMap.<String, Object>builder()
                .put("accessModes", singletonList("ReadWriteOnce"))
                .put("storageClassName", storageClass)
                .put("resources", ImmutableMap.of("requests", ImmutableMap.of("storage", size)))
                .build();
    }

    private V1beta1CustomResourceDefinition getPravegaCRD() {

        return new V1beta1CustomResourceDefinitionBuilder()
                .withApiVersion("apiextensions.k8s.io/v1beta1")
                .withKind("CustomResourceDefinition")
                .withMetadata(new V1ObjectMetaBuilder().withName("pravegaclusters.pravega.pravega.io").build())
                .withSpec(new V1beta1CustomResourceDefinitionSpecBuilder()
                                  .withGroup(CUSTOM_RESOURCE_GROUP_PRAVEGA)
                                  .withNames(new V1beta1CustomResourceDefinitionNamesBuilder()
                                                     .withKind(CUSTOM_RESOURCE_KIND_PRAVEGA)
                                                     .withListKind("PravegaClusterList")
                                                     .withPlural(CUSTOM_RESOURCE_PLURAL_PRAVEGA)
                                                     .withSingular("pravegacluster")
                                                     .build())
                                  .withScope("Namespaced")
                                  .withVersion(CUSTOM_RESOURCE_VERSION_PRAVEGA)
                                  .build())
                .build();

    }

    private V1beta1Role getPravegaOperatorRole() {
        return new V1beta1RoleBuilder()
                .withKind("Role")
                .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                .withMetadata(new V1ObjectMetaBuilder().withName(PRAVEGA_OPERATOR).build())
                .withRules(new V1beta1PolicyRuleBuilder().withApiGroups(CUSTOM_RESOURCE_GROUP_PRAVEGA)
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
                                                         .build())
                .build();
    }

    private V1beta1RoleBinding getPravegaOperatorRoleBinding() {
        return new V1beta1RoleBindingBuilder().withKind("RoleBinding")
                                              .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                                              .withMetadata(new V1ObjectMetaBuilder()
                                                                    .withName("default-account-pravega-operator")
                                                                    .build())
                                              .withSubjects(new V1beta1SubjectBuilder().withKind("ServiceAccount")
                                                                                       .withName(NAMESPACE)
                                                                                       .withNamespace(NAMESPACE)
                                                                                       .build())
                                              .withRoleRef(new V1beta1RoleRefBuilder().withKind("Role")
                                                                                      .withName(PRAVEGA_OPERATOR)
                                                                                      .withApiGroup("rbac.authorization.k8s.io")
                                                                                      .build())
                                              .build();
    }

    private V1Deployment getPravegaOperatorDeployment() {
        V1Container container = new V1ContainerBuilder().withName(PRAVEGA_OPERATOR)
                                                        .withImage("adrianmo/pravega-operator:" + PRAVEGA_OPERATOR_VERSION)
                                                        .withPorts(new V1ContainerPortBuilder().withContainerPort(60000).build())
                                                        .withCommand(PRAVEGA_OPERATOR)
                                                        // start the pravega-operator in test mode to disable minimum replica count check.
                                                        .withArgs("-test")
                                                        .withImagePullPolicy(IMAGE_PULL_POLICY)
                                                        .withEnv(new V1EnvVarBuilder().withName("WATCH_NAMESPACE")
                                                                                      .withValueFrom(new V1EnvVarSourceBuilder()
                                                                                                             .withFieldRef(new V1ObjectFieldSelectorBuilder()
                                                                                                                                   .withFieldPath("metadata.namespace")
                                                                                                                                   .build())
                                                                                                             .build())
                                                                                      .build(),
                                                                 new V1EnvVarBuilder().withName("OPERATOR_NAME")
                                                                                      .withValue(PRAVEGA_OPERATOR)
                                                                                      .build())
                                                        .build();
        return new V1DeploymentBuilder().withMetadata(new V1ObjectMetaBuilder().withName(PRAVEGA_OPERATOR)
                                                                               .withNamespace(NAMESPACE)
                                                                               .build())
                                        .withKind("Deployment")
                                        .withApiVersion("apps/v1")
                                        .withSpec(new V1DeploymentSpecBuilder().withMinReadySeconds(MIN_READY_SECONDS)
                                                                               .withReplicas(1)
                                                                               .withSelector(new V1LabelSelectorBuilder()
                                                                                                     .withMatchLabels(ImmutableMap.of("name", PRAVEGA_OPERATOR))
                                                                                                     .build())
                                                                               .withTemplate(new V1PodTemplateSpecBuilder()
                                                                                                     .withMetadata(new V1ObjectMetaBuilder()
                                                                                                                           .withLabels(ImmutableMap.of("name", PRAVEGA_OPERATOR))
                                                                                                                           .build())
                                                                                                     .withSpec(new V1PodSpecBuilder()
                                                                                                                       .withContainers(container)
                                                                                                                       .build())
                                                                                                     .build())
                                                                               .build())
                                        .build();
    }

    @Override
    public void clean() {
        // this is a NOP for KUBERNETES based implementation.
    }
}
