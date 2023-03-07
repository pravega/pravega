/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework.services.kubernetes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.models.V1ClusterRoleBinding;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1RoleRef;
import io.kubernetes.client.openapi.models.V1SecretVolumeSource;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1Subject;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.TestExecutor;
import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.kubernetes.ClientFactory;
import io.pravega.test.system.framework.kubernetes.K8sClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

@Slf4j
public class K8SequentialExecutor implements TestExecutor {

    private static final String APP = "pravega-system-tests";
    private static final String NAMESPACE = "default"; // KUBERNETES namespace where the tests run.
    private static final String SERVICE_ACCOUNT = System.getProperty("testServiceAccount", "test-framework"); //Service Account used by the test pod.
    private static final String CLUSTER_ROLE_BINDING = System.getProperty("testClusterRoleBinding", "cluster-admin-testFramework");
    private static final String TEST_POD_IMAGE = System.getProperty("testPodImage", "openjdk:8u181-jre-alpine");

    @Override
    public CompletableFuture<Void> startTestExecution(Method testMethod) {
        final String className = testMethod.getDeclaringClass().getName();
        final String methodName = testMethod.getName();
        // pod name is the combination of a test method name and random Alphanumeric. It cannot be more than 63 characters.
        final String podName = (methodName + "-" + randomAlphanumeric(5)).toLowerCase();
        log.info("Start execution of test {}#{} on the KUBERNETES Cluster", className, methodName);

        final K8sClient client = ClientFactory.INSTANCE.getK8sClient();

        Map<String, V1ContainerStatus> podStatusBeforeTest = getPravegaPodStatus(client);

        final V1Pod pod = getTestPod(className, methodName, podName.toLowerCase());
        final AtomicReference<CompletableFuture<Void>> logDownload = new AtomicReference<>(CompletableFuture.completedFuture(null));
        return client.createServiceAccount(NAMESPACE, getServiceAccount()) // create service Account, ignore if already present.
                .thenCompose(v -> client.createClusterRoleBinding(getClusterRoleBinding())) // ensure test pod has cluster admin rights.
                .thenCompose(v -> client.deployPod(NAMESPACE, pod)) // deploy test pod.
                .thenCompose(v -> {
                    // start download of logs.
                    if (!Utils.isSkipLogDownloadEnabled()) {
                        logDownload.set(client.downloadLogs(pod, "./build/test-results/" + podName));
                    }
                    return client.waitUntilPodCompletes(NAMESPACE, podName);
                }).handle((s, t) -> {
                       Futures.getAndHandleExceptions(logDownload.get(), t1 -> {
                           log.error("Failed to download logs for {}#{}", className, methodName, t1);
                           return null;
                       });
                    if (t == null) {
                        log.info("Test {}#{} execution completed with status {}", className, methodName, s);
                        verifyPravegaPodRestart(podStatusBeforeTest, getPravegaPodStatus(client));
                        if (s.getExitCode() != 0) {
                            log.error("Test {}#{} failed. Details: {}", className, methodName, s);
                            throw new AssertionError(methodName + " test failed due to " + s.getReason() + " with message " + s.getMessage());
                        } else {
                            return null;
                        }
                    } else {
                        throw new CompletionException("Error while invoking the test " + podName, t);
                    }
                });
    }

    private Map<String, V1ContainerStatus> getPravegaPodStatus(K8sClient client) {
        // fetch the status of pods deployed by Pravega-operator and zookeeper operator.
        Map<String, V1ContainerStatus> podStatusBeforeTest = getApplicationPodStatus(client, "app", "pravega-cluster");
        podStatusBeforeTest.putAll(getApplicationPodStatus(client, "app", "zookeeper"));
        return podStatusBeforeTest;
    }

    private Map<String, V1ContainerStatus> getApplicationPodStatus(K8sClient client, String labelName, String labelValue) {
        return Futures.getAndHandleExceptions(
                client.getRestartedPods(NAMESPACE, labelName, labelValue),
                t -> new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Failed to get status of Pravega pods", t));
    }

    private void verifyPravegaPodRestart(Map<String, V1ContainerStatus> podStatusBeforeTest, Map<String, V1ContainerStatus> podStatusAfterTest) {
        Map<String, V1ContainerStatus> restartMapFinal = podStatusAfterTest.entrySet().stream()
                .filter(e -> !podStatusBeforeTest.containsKey(e.getKey()) ||
                        podStatusBeforeTest.get(e.getKey()).getRestartCount() < e.getValue().getRestartCount())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (restartMapFinal.size() != 0) {
            log.error("Pravega pods have restarted, Details: {}", restartMapFinal);
            throw new AssertionError("Failing test due to Pravega pod restart.\n" + restartMapFinal);
        }
    }

    private V1Pod getTestPod(String className, String methodName, String podName) {
        log.info("Running test pod with security enabled :{}, transport enabled: {}", Utils.AUTH_ENABLED, Utils.TLS_AND_AUTH_ENABLED);
        V1Pod pod =  new V1Pod()
                .metadata(new V1ObjectMeta().name(podName).namespace(NAMESPACE).labels(ImmutableMap.of("POD_NAME", podName, "app", APP)))
                .spec( new V1PodSpec().serviceAccountName(SERVICE_ACCOUNT).automountServiceAccountToken(true)
                .volumes(ImmutableList.of(new V1Volume().name("task-pv-storage")
                    .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource().claimName("task-pv-claim"))
                    ))
                .containers( ImmutableList.of( new V1Container()
                .name(podName) // container name is same as that of the pod.
                .image(TEST_POD_IMAGE)
                .imagePullPolicy("IfNotPresent")
                .command(ImmutableList.of("/bin/sh"))
                .args( ImmutableList.of("-c", "java" +
                    getArgs() +
                    " -cp /data/test-collection.jar io.pravega.test.system.SingleJUnitTestRunner " + className + "#" + methodName /*+ " > server.log 2>&1 */ + "; exit $?"))
                .volumeMounts(ImmutableList.of(new V1VolumeMount().mountPath("/data").name("task-pv-storage")))
                ))
                .restartPolicy("Never"));
        if (Utils.TLS_AND_AUTH_ENABLED) {
            List<V1Volume> volumes = new ArrayList<>(pod.getSpec().getVolumes());
            volumes.add(new V1Volume().name("tls-cert").secret(new V1SecretVolumeSource().secretName(Utils.TLS_SECRET_NAME)));
            pod.getSpec().setVolumes(volumes);
            List<V1VolumeMount> volumeMounts = new ArrayList<>(pod.getSpec().getContainers().get(0).getVolumeMounts());
            volumeMounts.add(new V1VolumeMount().mountPath(Utils.TLS_MOUNT_PATH).name("tls-cert"));
            pod.getSpec().getContainers().get(0).setVolumeMounts(volumeMounts);
        }
        return pod;
    }

    private static String getArgs() {
        String[] args = new String[]{
                "execType",
                "dockerRegistryUrl",
                "skipServiceInstallation",
                "skipLogDownload",
                "imagePrefix",
                "pravegaImageName",
                "bookkeeperImageName",
                "zookeeperImageName",
                "zookeeperImageVersion",
                "imageVersion",
                "bookkeeperImageVersion",
                "tier2Type",
                "tier2Config",
                "pravegaOperatorVersion",
                "bookkeeperOperatorVersion",
                "zookeeperOperatorVersion",
                "publishedChartName",
                "helmRepository",
                "controllerLabel",
                "segmentstoreLabel",
                "tlsSecretName",
                "bookkeeperLabel",
                "pravegaID",
                "bookkeeperID",
                "bookkeeperConfigMap",
                "targetPravegaOperatorVersion",
                "targetBookkeeperOperatorVersion",
                "targetZookeeperOperatorVersion",
                "targetPravegaVersion",
                "targetBookkeeperVersion",
                "targetZookeeperVersion",
                "testPodImage",
                "testServiceAccount",
                "testClusterRoleBinding",
                "imageVersion",
                "securityEnabled",
                "tlsEnabled",
                "failFast",
                "log.level"
        };

        StringBuilder builder = new StringBuilder();
        for (String arg : args) {
            builder.append(String.format(" -D%s=%s", arg, Utils.getConfig(arg, "")));
        }
        return builder.toString();
    }

    @Override
    public void stopTestExecution() {
        throw new NotImplementedException("Not implemented for Kubernetes based tests");
    }

    private V1ServiceAccount getServiceAccount() {
        return new V1ServiceAccount()
                .apiVersion("v1")
                .kind("ServiceAccount")
                .metadata(new V1ObjectMeta()
                        .namespace(NAMESPACE)
                        .name(SERVICE_ACCOUNT));
    }

    private V1ClusterRoleBinding getClusterRoleBinding() {
        return new V1ClusterRoleBinding().kind("ClusterRoleBinding")
                .apiVersion("rbac.authorization.k8s.io/v1")
                .metadata(new V1ObjectMeta()
                        .name(CLUSTER_ROLE_BINDING)
                        .namespace(NAMESPACE))
                .subjects(ImmutableList.of(new V1Subject().kind("ServiceAccount")
                        .name(SERVICE_ACCOUNT)
                        .namespace(NAMESPACE)))
                .roleRef(new V1RoleRef().kind("ClusterRole")
                        .name("cluster-admin")
                        .apiGroup("")); // all core apis.
    }
}
