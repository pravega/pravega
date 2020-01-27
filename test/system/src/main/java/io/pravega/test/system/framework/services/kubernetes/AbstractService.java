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

import com.google.common.base.Splitter;
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
import io.kubernetes.client.models.V1beta1CustomResourceDefinitionStatus;
import io.kubernetes.client.models.V1beta1PolicyRuleBuilder;
import io.kubernetes.client.models.V1beta1Role;
import io.kubernetes.client.models.V1beta1ClusterRole;
import io.kubernetes.client.models.V1beta1RoleBinding;
import io.kubernetes.client.models.V1beta1ClusterRoleBinding;
import io.kubernetes.client.models.V1beta1RoleBindingBuilder;
import io.kubernetes.client.models.V1beta1ClusterRoleBindingBuilder;
import io.kubernetes.client.models.V1beta1RoleBuilder;
import io.kubernetes.client.models.V1beta1ClusterRoleBuilder;
import io.kubernetes.client.models.V1beta1RoleRefBuilder;
import io.kubernetes.client.models.V1beta1SubjectBuilder;
import io.pravega.test.system.framework.kubernetes.ClientFactory;
import io.pravega.test.system.framework.kubernetes.K8sClient;
import io.pravega.test.system.framework.services.Service;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.Exceptions.checkNotNullOrEmpty;
import static java.util.Collections.singletonList;

@Slf4j
public abstract class AbstractService implements Service {

    public static final int CONTROLLER_GRPC_PORT = 9090;
    public static final int CONTROLLER_REST_PORT = 10080;
    protected static final String DOCKER_REGISTRY =  System.getProperty("dockerRegistryUrl", "");
    protected static final String PREFIX = System.getProperty("imagePrefix", "pravega");
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
    static final String CUSTOM_RESOURCE_API_VERSION = CUSTOM_RESOURCE_GROUP_PRAVEGA + "/" + CUSTOM_RESOURCE_VERSION_PRAVEGA;
    static final String CUSTOM_RESOURCE_PLURAL_PRAVEGA = "pravegaclusters";
    static final String CUSTOM_RESOURCE_KIND_PRAVEGA = "PravegaCluster";
    static final String PRAVEGA_CONTROLLER_LABEL = "pravega-controller";
    static final String PRAVEGA_SEGMENTSTORE_LABEL = "pravega-segmentstore";
    static final String BOOKKEEPER_LABEL = "bookie";
    static final String PRAVEGA_ID = "pravega";
    static final String ZOOKEEPER_OPERATOR_IMAGE = System.getProperty("zookeeperOperatorImage", "pravega/zookeeper-operator:latest");
    static final String IMAGE_PULL_POLICY = System.getProperty("imagePullPolicy", "Always");
    private static final String PRAVEGA_VERSION = System.getProperty("imageVersion", "latest");
    private static final String PRAVEGA_BOOKKEEPER_VERSION = System.getProperty("pravegaBookkeeperVersion", PRAVEGA_VERSION);
    private static final String PRAVEGA_OPERATOR_IMAGE = System.getProperty("pravegaOperatorImage", "pravega/pravega-operator:latest");
    private static final String PRAVEGA_IMAGE_NAME = System.getProperty("pravegaImageName", "pravega");
    private static final String BOOKKEEPER_IMAGE_NAME = System.getProperty("bookkeeperImageName", "bookkeeper");
    private static final String TIER2_NFS = "nfs";
    private static final String TIER2_TYPE = System.getProperty("tier2Type", TIER2_NFS);
    private static final boolean IS_OPERATOR_VERSION_ABOVE_040 = isOperatorVersionAbove040();

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

    CompletableFuture<Object> deployPravegaUsingOperator(final URI zkUri, int controllerCount, int segmentStoreCount, int bookieCount, ImmutableMap<String, String> props) {
    return k8sClient.createCRD(getPravegaCRD())
                        .thenCompose(v -> k8sClient.createRole(NAMESPACE, getPravegaOperatorRole()))
                        .thenCompose(v -> k8sClient.createClusterRole(getPravegaOperatorClusterRole()))
                        .thenCompose(v -> k8sClient.createRoleBinding(NAMESPACE, getPravegaOperatorRoleBinding()))
                        .thenCompose(v -> k8sClient.createClusterRoleBinding(getPravegaOperatorClusterRoleBinding()))
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
                                                                                                   bookieCount, props)));
    }

    private Map<String, Object> getPravegaDeployment(String zkLocation, int controllerCount, int segmentStoreCount, int bookieCount, ImmutableMap<String, String> props) {
        // generate BookkeeperSpec.
        final Map<String, Object> bkPersistentVolumeSpec = getPersistentVolumeClaimSpec("10Gi", "standard");

        // generate Pravega Spec.
        final Map<String, Object> pravegaPersistentVolumeSpec = getPersistentVolumeClaimSpec("20Gi", "standard");
        
        final String bookkeeperImg = DOCKER_REGISTRY + PREFIX + "/" + BOOKKEEPER_IMAGE_NAME;
        final String pravegaImg = DOCKER_REGISTRY + PREFIX + "/" + PRAVEGA_IMAGE_NAME;
        final Map<String, Object> bookkeeperImgSpec;
        final Map<String, Object> pravegaImgSpec;

        if (IS_OPERATOR_VERSION_ABOVE_040) {
            bookkeeperImgSpec = ImmutableMap.of("repository", bookkeeperImg);
            pravegaImgSpec = ImmutableMap.of("repository", pravegaImg);
        } else {
            bookkeeperImgSpec = getImageSpec(bookkeeperImg, PRAVEGA_BOOKKEEPER_VERSION);
            pravegaImgSpec = getImageSpec(pravegaImg, PRAVEGA_VERSION);
        }
        
        final Map<String, Object> bookkeeperSpec = ImmutableMap.<String, Object>builder().put("image", bookkeeperImgSpec)
                .put("replicas", bookieCount)
                .put("resources", getResources("2000m", "5Gi", "1000m", "3Gi"))
                .put("storage", ImmutableMap.builder()
                        .put("indexVolumeClaimTemplate", bkPersistentVolumeSpec)
                        .put("ledgerVolumeClaimTemplate", bkPersistentVolumeSpec)
                        .put("journalVolumeClaimTemplate", bkPersistentVolumeSpec)
                        .build())
                .put("autoRecovery", true)
                .build();

        final Map<String, Object> pravegaSpec = ImmutableMap.<String, Object>builder().put("controllerReplicas", controllerCount)
                .put("segmentStoreReplicas", segmentStoreCount)
                .put("debugLogging", true)
                .put("cacheVolumeClaimTemplate", pravegaPersistentVolumeSpec)
                .put("controllerResources", getResources("2000m", "3Gi", "1000m", "1Gi"))
                .put("segmentStoreResources", getResources("2000m", "5Gi", "1000m", "3Gi"))
                .put("options", props)
                .put("image", pravegaImgSpec)
                .put("tier2", tier2Spec())
                .build();

        return ImmutableMap.<String, Object>builder()
                .put("apiVersion", CUSTOM_RESOURCE_API_VERSION)
                .put("kind", CUSTOM_RESOURCE_KIND_PRAVEGA)
                .put("metadata", ImmutableMap.of("name", PRAVEGA_ID, "namespace", NAMESPACE))
                .put("spec", buildPravegaClusterSpec(zkLocation, bookkeeperSpec, pravegaSpec))
                .build();
    }

    /**
     * As upgrade has been included in operator 0.4.0 a few changes have been included in
     * the Pravega Cluster Spec like the image tag field for Pravega and Bookeeper has
     * been replaced by a single version field which indicates the version for both the
     * Pravega and Bookeeper components. Hence determining whether the operator version to
     * be used for running the System Tests is below 0.4.0 or not is crucial.
     *
     * @return true if operator version used is greater or equal to 0.4.0, false otherwise.
     */
    private static boolean isOperatorVersionAbove040() {
        String pravegaOperatorTag = PRAVEGA_OPERATOR_IMAGE.substring(PRAVEGA_OPERATOR_IMAGE.lastIndexOf(":") + 1);
        if (!pravegaOperatorTag.equals("latest")) {
            String version = pravegaOperatorTag.substring(0, pravegaOperatorTag.lastIndexOf("."));
            float v = Float.parseFloat(version);
            if (v < 0.4) {
                return false;
            }
        }
        return true;
    }

    protected Map<String, Object> buildPravegaClusterSpec(String zkLocation, Map<String, Object> bookkeeperSpec, Map<String, Object> pravegaSpec) {

        ImmutableMap<String, Object> commonEntries = ImmutableMap.<String, Object>builder()
                .put("zookeeperUri", zkLocation)
                .put("bookkeeper", bookkeeperSpec)
                .put("pravega", pravegaSpec)
                .build();

        if (IS_OPERATOR_VERSION_ABOVE_040) {
            return ImmutableMap.<String, Object>builder()
                    .putAll(commonEntries)
                    .put("version", PRAVEGA_VERSION)
                    .build();
        }
        return commonEntries;

    }

    /**
     * Helper method to create the Pravega Cluster Spec which specifies just those values in the
     * spec which need to be patched. Other values remain same as were specified at the time of
     * deployment.
     * @param service Name of the service to be patched (bookkeeper/ segment store/ controller).
     * @param replicaCount Number of replicas.
     * @param component Name of the component (pravega/ bookkeeper).
     *
     * @return the new Pravega Cluster Spec containing the values that need to be patched.
     */
    protected static Map<String, Object> buildPatchedPravegaClusterSpec(String service, int replicaCount, String component) {

        final Map<String, Object> componentSpec = ImmutableMap.<String, Object>builder()
                .put(service, replicaCount)
                .build();

        return ImmutableMap.<String, Object>builder()
                .put("apiVersion", CUSTOM_RESOURCE_API_VERSION)
                .put("kind", CUSTOM_RESOURCE_KIND_PRAVEGA)
                .put("metadata", ImmutableMap.of("name", PRAVEGA_ID, "namespace", NAMESPACE))
                .put("spec", ImmutableMap.builder()
                        .put(component, componentSpec)
                        .build())
                .build();
    }

    private Map<String, Object> tier2Spec() {
        final Map<String, Object> spec;
        if (TIER2_TYPE.equalsIgnoreCase(TIER2_NFS)) {
            spec = ImmutableMap.of("filesystem", ImmutableMap.of("persistentVolumeClaim",
                                                                 ImmutableMap.of("claimName", "pravega-tier2")));
        } else {
            // handle other types of tier2 like HDFS and Extended S3 Object Store.
            spec = ImmutableMap.of(TIER2_TYPE, getTier2Config());
        }
        return spec;
    }

    private Map<String, Object> getTier2Config() {
        String tier2Config = System.getProperty("tier2Config");
        checkNotNullOrEmpty(tier2Config, "tier2Config");
        Map<String, String> split = Splitter.on(',').trimResults().withKeyValueSeparator("=").split(tier2Config);
        return split.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            try {
                return Integer.parseInt(e.getValue());
            } catch (NumberFormatException ex) {
                // return all non integer configuration as String.
                return e.getValue();
            }
        }));
    }

    private Map<String, Object> getPersistentVolumeClaimSpec(String size, String storageClass) {
        return ImmutableMap.<String, Object>builder()
                .put("accessModes", singletonList("ReadWriteOnce"))
                .put("storageClassName", storageClass)
                .put("resources", ImmutableMap.of("requests", ImmutableMap.of("storage", size)))
                .build();
    }

    protected Map<String, Object> getImageSpec(String imageName, String tag) {
        return ImmutableMap.<String, Object>builder().put("repository", imageName)
                .put("tag", tag)
                .put("pullPolicy", IMAGE_PULL_POLICY)
                .build();
    }

    private Map<String, Object> getResources(String limitsCpu, String limitsMem, String requestsCpu, String requestsMem) {
        return ImmutableMap.<String, Object>builder()
                .put("limits", ImmutableMap.builder()
                        .put("cpu", limitsCpu)
                        .put("memory", limitsMem)
                        .build())
                .put("requests", ImmutableMap.builder()
                        .put("cpu", requestsCpu)
                        .put("memory", requestsMem)
                        .build())
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
                                  .withNewSubresources()
                                  .withStatus(new V1beta1CustomResourceDefinitionStatus())
                                  .endSubresources()
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

    private V1beta1ClusterRole getPravegaOperatorClusterRole() {
        return new V1beta1ClusterRoleBuilder()
                .withKind("ClusterRole")
                .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                .withMetadata(new V1ObjectMetaBuilder().withName(PRAVEGA_OPERATOR).build())
                .withRules(new V1beta1PolicyRuleBuilder().withApiGroups("")
                                .withResources("nodes")
                                .withVerbs("get", "watch", "list")
                                .build(),
                        new V1beta1PolicyRuleBuilder().withApiGroups("admissionregistration.k8s.io")
                                .withResources("*")
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

    private V1beta1ClusterRoleBinding getPravegaOperatorClusterRoleBinding() {
        return new V1beta1ClusterRoleBindingBuilder().withKind("ClusterRoleBinding")
                .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                .withMetadata(new V1ObjectMetaBuilder()
                        .withName("default-account-pravega-operator")
                        .build())
                .withSubjects(new V1beta1SubjectBuilder().withKind("ServiceAccount")
                        .withName(NAMESPACE)
                        .withNamespace(NAMESPACE)
                        .build())
                .withRoleRef(new V1beta1RoleRefBuilder().withKind("ClusterRole")
                        .withName(PRAVEGA_OPERATOR)
                        .withApiGroup("rbac.authorization.k8s.io")
                        .build())
                .build();
    }

    private V1Deployment getPravegaOperatorDeployment() {
        V1Container container = new V1ContainerBuilder().withName(PRAVEGA_OPERATOR)
                                                        .withImage(PRAVEGA_OPERATOR_IMAGE)
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
