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
package io.pravega.test.system.framework;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.VersionCompare;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;

import static io.pravega.test.system.framework.Utils.DOCKER_NETWORK;
import static io.pravega.test.system.framework.Utils.getConfig;
import static org.junit.Assert.assertFalse;

@Slf4j
public class DockerBasedTestExecutor implements TestExecutor {

    public static final int DOCKER_CLIENT_PORT = 2375;
    private final static String IMAGE = "java:8";
    private static final String LOG_LEVEL = System.getProperty("log.level", "DEBUG");
    private final AtomicReference<String> id = new AtomicReference<String>();
    private final String masterIp = Utils.isAwsExecution() ? getConfig("awsMasterIP", "Invalid Master IP").trim() : getConfig("masterIP", "Invalid Master IP");
    private final DockerClient client = DefaultDockerClient.builder().uri("http://" + masterIp
            + ":" + DOCKER_CLIENT_PORT).build();
    private final String expectedDockerApiVersion = "1.22";
    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(3, "test");

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executorService);
    }

    @Override
    public CompletableFuture<Void> startTestExecution(Method testMethod) {

        try {
            final String dockerApiVersion = Exceptions.handleInterruptedCall(() -> client.version().apiVersion());
            if (!(VersionCompare.compareVersion(dockerApiVersion, expectedDockerApiVersion) >= 0)) {
                throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Docker API doesnt match." +
                        "Cannot Invoke Tests.Excepected = " + expectedDockerApiVersion + "Actual = " + dockerApiVersion);
            }
        } catch (DockerException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed,
                    "Unable to find docker client version.Cannot continue test execution.", e);
        }

        log.debug("Starting test execution for method: {}", testMethod);

        String className = testMethod.getDeclaringClass().getName();
        String methodName = testMethod.getName();
        String containerName = methodName + ".testJob";

        return CompletableFuture.runAsync(() -> {
            startTest(containerName, className, methodName);
        }).thenCompose(v2 -> waitForJobCompletion())
                .<Void>thenApply(v1 -> {
                    try {
                        if (Exceptions.handleInterruptedCall(() -> client.inspectContainer(id.get()).state().exitCode() != 0)) {
                            throw new AssertionError("Test failed "
                                    + className + "#" + methodName);
                        }
                        ExecutorServiceHelpers.shutdown(executorService);
                    } catch (DockerException e) {
                        throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Unable to get " +
                                "container exit status and test result.", e);
                    }
                    return null;
                }).whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("Error while executing the test. ClassName: {}, MethodName: {}", className,
                                methodName);
                    }
                });
    }

    @Override
    public void stopTestExecution() {
        try {
            Exceptions.handleInterrupted(() -> client.stopContainer(id.get(), 0));
        } catch (DockerException e) {
            log.error("Unable to stop the test execution", e);
        }
    }

    private CompletableFuture<Void> waitForJobCompletion() {
        return Futures.loop(() -> isTestRunning(),
                () -> Futures.delayedFuture(Duration.ofSeconds(3), executorService),
                executorService);
    }

    private boolean isTestRunning() {
        boolean value = false;
        try {
            if (Exceptions.handleInterruptedCall(() -> client.inspectContainer(this.id.get()).state().running())) {
                value = true;
            }
        } catch (DockerException e) {
            log.error("Unable to list docker services", e);
        }
        return value;
    }

    private String startTest(String containerName, String className, String methodName) {

        try {
            Exceptions.handleInterrupted(() -> client.pull(IMAGE));

            ContainerCreation containerCreation = Exceptions.handleInterruptedCall(() -> client.
                    createContainer(setContainerConfig(methodName, className), containerName));
            assertFalse(containerCreation.id().toString().equals(null));

            id.set(containerCreation.id());

            final Path dockerDirectory = Paths.get(System.getProperty("user.dir"));
            try {
                //Copy the test.jar with all the dependencies into the container.
                Exceptions.handleInterrupted(() -> client.copyToContainer(dockerDirectory, id.get(), "/data"));
            } catch (Exception e) {
                throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Unable to copy test jar " +
                        "to the container.Test failure", e);
            }

            String networkId = Exceptions.handleInterruptedCall(() -> client.listNetworks(DockerClient.ListNetworksParam.
                    byNetworkName(DOCKER_NETWORK)).get(0).id());
            //Container should be connect to the user-defined overlay network to communicate with all the services deployed.
            Exceptions.handleInterrupted(() -> client.connectToNetwork(id.get(), networkId));

            // Start container
            Exceptions.handleInterrupted(() -> client.startContainer(id.get()));

        } catch (DockerException e) {
            log.error("Exception in starting container ", e);
            Assert.fail("Unable to start the container to invoke the test.Test failure");
        }
        return id.get();
    }

    private ContainerConfig setContainerConfig(String methodName, String className) {

        Map<String, String> labels = new HashMap<>(2);
        labels.put("testMethodName", methodName);
        labels.put("testClassName", className);

        HostConfig hostConfig = HostConfig.builder()
                .portBindings(ImmutableMap.of(DOCKER_CLIENT_PORT + "/tcp", Arrays.asList(PortBinding.
                        of(LoginClient.MESOS_MASTER, DOCKER_CLIENT_PORT)))).networkMode("docker_gwbridge").build();

        ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image(IMAGE)
                .user("root")
                .workingDir("/data")
                .cmd("sh", "-c", "java -DmasterIP=" + LoginClient.MESOS_MASTER + " -DexecType=" + getConfig("execType",
                        "LOCAL") + " -Dlog.level=" + LOG_LEVEL +  " -cp /data/build/libs/test-docker-collection.jar io.pravega.test.system." +
                        "SingleJUnitTestRunner " + className + "#" + methodName + " > server.log 2>&1")
                .labels(labels)
                .build();

        return containerConfig;
    }
}
