/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import io.pravega.common.concurrent.FutureHelpers;
import lombok.extern.slf4j.Slf4j;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static org.junit.Assert.assertFalse;

@Slf4j
public class DockerRemoteSequential implements TestExecutor {

    public static final DockerClient CLIENT = DefaultDockerClient.builder().uri("http://"+ System.getProperty("masterIP")+ ":2375").build();

    private static final String IMAGE = System.getProperty("dockerImageRegistry")+ "/java:8";
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    public CompletableFuture<Void> startTestExecution(Method testMethod) {

        log.debug("Starting test execution for method: {}", testMethod);

        String className = testMethod.getDeclaringClass().getName();
        String methodName = testMethod.getName();
        String containerName = methodName + ".testJob";

        return CompletableFuture.runAsync(() -> {
            createContainer(containerName, className, methodName);
        }).thenCompose(v2 -> waitForJobCompletion(containerName))
                .whenComplete((v, ex) -> {
                    deleteContainer(containerName); //delete container once execution is complete.
                    if (ex != null) {
                        log.error("Error while executing the test. ClassName: {}, MethodName: {}", className,
                                methodName);
                    }
                }
                );
    }

    @Override
    public CompletableFuture<Void> stopTestExecution(String testID) {
        throw new org.apache.commons.lang.NotImplementedException("Stop Execution is not used for Remote sequential execution");
    }

    private CompletableFuture<Void> waitForJobCompletion(final String containerName) {
        return FutureHelpers.loop(() -> isTestRunning(containerName),
                () -> FutureHelpers.delayedFuture(Duration.ofSeconds(3), executorService),
                executorService);
    }

    private boolean isTestRunning(String containerName) {
        boolean value = false;
        try {
            //list the service with filter 'serviceName'
            List<Container> containers = CLIENT.listContainers(DockerClient.ListContainersFilterParam.withStatusRunning());
            for (int i = 0; i < containers.size(); i++) {
                ContainerInfo info = CLIENT.inspectContainer(containers.get(i).id());
                if (info.name().equals(containerName)) {
                    value = true;
                    break;
                }
            }
        } catch (DockerException | InterruptedException e) {
            log.error("unable to list docker services {}", e);
        }
        return value;
    }

    private void createContainer(String containerName, String className, String methodName) {

        try {
            CLIENT.pull(IMAGE);

            ContainerCreation containerCreation = CLIENT.createContainer(setContainerConfig(methodName, className), containerName);
            assertFalse(containerCreation.id().toString().equals(null));

             String id = containerCreation.id();

            // Inspect container
            final ContainerInfo info = CLIENT.inspectContainer(id);

            //TODO: copy test.jar to the container
            /*try {
                final Path dockerDirectory = Paths.get(Resources.getResource(System.getProperty("user.dir")+ "/build/libs").toURI());
                CLIENT.copyToContainer(dockerDirectory, id, "/data");
            } catch(URISyntaxException | IOException e) {
                log.error("Exception while copying test jar to the container {}", e);
            }*/

            // Start container
            CLIENT.startContainer(id);

        } catch (DockerException | InterruptedException e) {
            log.error("exception in starting container {}", e);
        }
    }

    private ContainerConfig setContainerConfig(String methodName, String className) {

        Map<String, String> labels = new HashMap<>(1);
        labels.put("testMethodName", methodName);
        labels.put("testClassName", className);

        HostConfig hostConfig = HostConfig.builder().appendBinds(System.getProperty("user.dir") + "/build/libs:/data")
                .portBindings(ImmutableMap.of( "2375/tcp", Arrays.asList( PortBinding.of( System.getProperty("masterIP"), "2375" ) ) ) ).networkMode("host").build();

        ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image(IMAGE)
                .user("root")
                .workingDir("/data")
                .cmd("sh", "-c", "java -DdockerExecLocal="+ Utils.isDockerLocalExecEnabled()+ " -DmasterIP="+System.getProperty("masterIP")+ " -DskipServiceInstallation=" + Utils.isSkipServiceInstallationEnabled() + " -cp /data/test-collection.jar io.pravega.test.system.SingleJUnitTestRunner " +
                        className + "#" + methodName + " > "+  className + "#" + methodName + "server.log 2>&1" + "; exit $?")
                .labels(labels)
                .build();

        return containerConfig;
    }

    private void deleteContainer(String containerName) {
        try {
            List<Container> containers = CLIENT.listContainers(DockerClient.ListContainersParam.allContainers());
            for (int i = 0; i < containers.size(); i++) {
                ContainerInfo info = CLIENT.inspectContainer(containers.get(i).id());
                if (info.name().equals(containerName)) {
                    CLIENT.removeContainer(info.id());
                }
            }
        } catch (DockerException | InterruptedException e) {
            log.error("unable to get service list {}", e);
        }

    }
}


