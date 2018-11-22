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
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.TestFrameworkException;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;
import static java.util.Collections.singletonList;

@Slf4j
public class PravegaControllerService extends AbstractService {

    private final URI zkUri;

    public PravegaControllerService(final String id, final URI zkUri) {
        super(id);
        this.zkUri = zkUri;
    }

    @Override
    public void start(boolean wait) {
        Futures.getAndHandleExceptions(k8Client.createCRD(getPravegaCRD())
                                               .thenCompose(v -> k8Client.createRole(NAMESPACE, getRole()))
                                               .thenCompose(v -> k8Client.createRoleBinding(NAMESPACE, getRoleBinding()))
                                               //deploy pravega operator.
                                               .thenCompose(v -> k8Client.createDeployment(NAMESPACE, getPravegaOperatorDeployment()))
                                               // wait until pravega operator is running, only one instance of operator is running.
                                               .thenCompose(v -> k8Client.waitUntilPodIsRunning(NAMESPACE, "name", PRAVEGA_OPERATOR, 1))
                                               // request operator to deploy zookeeper nodes.
                                               .thenCompose(v -> k8Client.createAndUpdateCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA, CUSTOM_RESOURCE_VERSION_PRAVEGA,
                                                                                                      NAMESPACE, CUSTOM_RESOURCE_PLURAL_PRAVEGA,
                                                                                                      getPravegaDeployment(zkUri.getAuthority(),
                                                                                                                           DEFAULT_CONTROLLER_COUNT,
                                                                                                                           DEFAULT_SEGMENTSTORE_COUNT,
                                                                                                                           DEFAULT_BOOKIE_COUNT))),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to deploy pravega operator/pravega services", t));
        if (wait) {
            Futures.getAndHandleExceptions(k8Client.waitUntilPodIsRunning(NAMESPACE, "component", PRAVEGA_CONTROLLER_LABEL, DEFAULT_CONTROLLER_COUNT),
                                           t -> new TestFrameworkException(RequestFailed, "Failed to deploy pravega-controller service, check the operator logs", t));
        }
    }

    private Map<String, Object> getPravegaDeployment(String zkLocation, int controllerCount, int segmentStoreCount, int bookieCount) {

        // generate BookkeeperSpec.
        final Map<String, Object> bkPersistentVolumeSpec = getPersistentVolumeClaimSpec("1Gi", "standard");
        final Map<String, Object> bookeeperSpec = ImmutableMap.<String, Object>builder().put("image", getImageSpec("pravega/bookkeeper", "latest"))
                                                                                        .put("replicas", bookieCount)
                                                                                        .put("storage", ImmutableMap.builder()
                                                                                                                    .put("ledgerVolumeClaimTemplate", bkPersistentVolumeSpec)
                                                                                                                    .put("journalVolumeClaimTemplate", bkPersistentVolumeSpec)
                                                                                                                    .build())
                                                                                        .put("autoRecovery", true)
                                                                                        .build();

        // generate Pravega Spec.
        final Map<String, Object> pravegaPersistentVolumeSpec = getPersistentVolumeClaimSpec("2Gi", "standard");
        final Map<String, Object> pravegaSpec = ImmutableMap.<String, Object>builder().put("controllerReplicas", controllerCount)
                                                                                      .put("segmentStoreReplicas", segmentStoreCount)
                                                                                      .put("debugLogging", true)
                                                                                      .put("cacheVolumeClaimTemplate", pravegaPersistentVolumeSpec)
                                                                                      .put("options", ImmutableMap.of("log.level", "DEBUG"))
                                                                                      .put("image", getImageSpec("pravega/pravega", "latest"))
                                                                                      //.put("tier2", tier2Spec("pravega-tier2"))
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
                                                     .put("pullPolicy", DEFAULT_IMAGE_PULL_POLICY)
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


    @Override
    public void stop() {
        Futures.getAndHandleExceptions(k8Client.deleteCustomObject(CUSTOM_RESOURCE_GROUP_PRAVEGA, CUSTOM_RESOURCE_VERSION_PRAVEGA,
                                                                   NAMESPACE, CUSTOM_RESOURCE_PLURAL_PRAVEGA, PRAVEGA_ID),
                                       t -> new TestFrameworkException(RequestFailed, "Failed to stop pravega", t));

    }

    @Override
    public void clean() {

    }


    @Override
    public boolean isRunning() {
        return k8Client.getStatusOfPodWithLabel(NAMESPACE, "component", PRAVEGA_CONTROLLER_LABEL)
                       .thenApply(statuses -> statuses.stream()
                                                      .filter(podStatus -> podStatus.getContainerStatuses()
                                                                                    .stream()
                                                                                    .allMatch(st -> st.getState().getRunning() != null))
                                                      .count())
                       .thenApply(runCount -> runCount == DEFAULT_CONTROLLER_COUNT)
                       .exceptionally(t -> {
                           log.warn("Exception observed while checking status of pod " + PRAVEGA_CONTROLLER_LABEL, t);
                           return false;
                       }).join();
    }

    @Override
    public List<URI> getServiceDetails() {
        return null;
    }

    @Override
    public CompletableFuture<Void> scaleService(int instanceCount) {
        return null;
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

    private V1beta1Role getRole() {
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

    private V1beta1RoleBinding getRoleBinding() {
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
                                                        .withImage("pravega/pravega-operator:latest")
                                                        .withPorts(new V1ContainerPortBuilder().withContainerPort(60000).build())
                                                        .withCommand(PRAVEGA_OPERATOR)
                                                        .withImagePullPolicy(DEFAULT_IMAGE_PULL_POLICY)
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

    public static void main(String[] args) {
        ZookeeperService zkSer = new ZookeeperService();
        if (!zkSer.isRunning()) {
            zkSer.start(true);
        }
        URI zkUri = zkSer.getServiceDetails().get(0);
        System.out.println("===> " + zkUri);
        PravegaControllerService ser = new PravegaControllerService("controller", zkUri);

        if (!ser.isRunning()) {
            ser.start(true);
        }

    }
}
