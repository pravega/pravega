package io.pravega.test.system.framework.kubernetes;

import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerBuilder;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1ContainerPortBuilder;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentBuilder;
import io.kubernetes.client.models.V1DeploymentSpec;
import io.kubernetes.client.models.V1DeploymentSpecBuilder;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1EnvVarBuilder;
import io.kubernetes.client.models.V1EnvVarSource;
import io.kubernetes.client.models.V1EnvVarSourceBuilder;
import io.kubernetes.client.models.V1LabelSelectorBuilder;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectFieldSelectorBuilder;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodSpecBuilder;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1PodTemplateSpec;
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
import io.kubernetes.client.models.V1beta1RoleRef;
import io.kubernetes.client.models.V1beta1RoleRefBuilder;
import io.kubernetes.client.models.V1beta1Subject;
import io.kubernetes.client.models.V1beta1SubjectBuilder;
import io.pravega.common.concurrent.Futures;
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

    private final static String DEFAULT_NS = "default";
    private final K8Client k8Client;

    public ZookeeperServiceOnK8() {
        k8Client = new K8Client();
    }


    @Override
    public void start(boolean wait) {

    }

    @Override
    public void stop() {

    }

    @Override
    public void clean() {

    }

    @Override
    public String getID() {
        return null;
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public List<URI> getServiceDetails() {
        return null;
    }

    @Override
    public CompletableFuture<Void> scaleService(int instanceCount) {
        return null;
    }


    public void verifyZkOperator() throws IOException, ApiException {
        CompletableFuture<V1PodStatus> r = k8Client.getStatusOfPod(DEFAULT_NS, "zookeeper-operator");
        Futures.await(r);
        log.error("ZK operator {}", r.join());
        if (!Futures.isSuccessful(r)) {

        }
    }

    public void test() throws Exception {
//        CompletableFuture<V1beta1CustomResourceDefinition> r1 = k8Client.createCRD(getCustomResourceDefnition());
//        Futures.await(r1);
//
//        CompletableFuture<V1beta1ClusterRole> r2 = k8Client.createClusterRole(getClusterRole());
//        Futures.await(r2);
//        CompletableFuture<V1beta1ClusterRoleBinding> r3 = k8Client.createClusterRoleBinding(getClusterRoleBinding());
//        Futures.await(r3);
//        CompletableFuture<V1Deployment> r4 = k8Client.createDeployment("default", getDeployment("default"));
//        Futures.await(r4);
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("apiVersion", "zookeeper.pravega.io/v1beta1")
                .put("kind", "ZookeeperCluster")
                .put("metadata", ImmutableMap.of("name", "example"))
                .put("spec", ImmutableMap.of("size", 2))
                .build();

        Futures.await(k8Client.createAndUpdateCustomObject("zookeeper.pravega.io", "v1beta1", "default", "zookeeper-clusters", map));

        System.out.println("finish test");
    }

    private V1beta1ClusterRoleBinding getClusterRoleBinding() {
        return new V1beta1ClusterRoleBindingBuilder().withKind("ClusterRoleBinding")
                                                     .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                                                     .withMetadata(new V1ObjectMetaBuilder()
                                                                           .withName("default-account-zookeeper-operator")
                                                                           .build())
                                                     .withSubjects(new V1beta1SubjectBuilder().withKind("ServiceAccount")
                                                                                              .withName("default")
                                                                                              .withNamespace("default")
                                                                                              .build())
                                                     .withRoleRef(new V1beta1RoleRefBuilder().withKind("ClusterRole")
                                                                                             .withName("zookeeper-operator")
                                                                                             .withApiGroup("rbac.authorization.k8s.io")
                                                                                             .build())
                                                     .build();
    }

    private V1beta1CustomResourceDefinition getCustomResourceDefnition() {
        final V1beta1CustomResourceDefinition def = new V1beta1CustomResourceDefinitionBuilder()
                .withApiVersion("apiextensions.k8s.io/v1beta1")
                .withKind("CustomResourceDefinition")
                .withMetadata(new V1ObjectMetaBuilder().withName("zookeeper-clusters.zookeeper.pravega.io").build())
                .withSpec(new V1beta1CustomResourceDefinitionSpecBuilder()
                                  .withGroup("zookeeper.pravega.io")
                                  .withNames(new V1beta1CustomResourceDefinitionNamesBuilder()
                                                     .withKind("ZookeeperCluster")
                                                     .withListKind("ZookeeperClusterList")
                                                     .withPlural("zookeeper-clusters")
                                                     .withSingular("zookeeper-cluster")
                                                     .withShortNames("zk")
                                                     .build())
                                  .withScope("Namespaced")
                                  .withVersion("v1beta1")
                                  .build())
                .build();
        return def;

    }

    private V1beta1ClusterRole getClusterRole() {
        final V1beta1ClusterRole role = new V1beta1ClusterRoleBuilder()
                .withKind("ClusterRole")
                .withApiVersion("rbac.authorization.k8s.io/v1beta1")
                .withMetadata(new V1ObjectMetaBuilder().withName("zookeeper-operator").build())
                .withRules(new V1beta1PolicyRuleBuilder().withApiGroups("zookeeper.pravega.io")
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


    private V1Deployment getDeployment(String namespace) {
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
                                                                               .withNamespace(namespace)
                                                                               .build())
                                        .withKind("Deployment")
                                        .withApiVersion("apps/v1")
                                        .withSpec(new V1DeploymentSpecBuilder().withReplicas(1)
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

    public static void main(String[] args) throws Exception {
        ZookeeperServiceOnK8 zkService = new ZookeeperServiceOnK8();
        zkService.test();


//        zkService.verifyZkOperator();

    }
}
