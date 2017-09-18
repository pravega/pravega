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
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.test.system.framework.docker.Client;
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

    public static final DockerClient CLIENT = Client.getDockerClient();
    private static final String IMAGE = "java:8";
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
                });
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

             final String id = containerCreation.id();

            // Inspect container
            final ContainerInfo info = CLIENT.inspectContainer(id);

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

        HostConfig hostConfig = HostConfig.builder().appendBinds("/root/git/docker-framework/pravega/test/system/build/libs:/data")
                .portBindings(ImmutableMap.of( "2375/tcp", Arrays.asList( PortBinding.of( "127.0.0.1", "2375" ) ) ) ).networkMode("host").build();

        ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image(IMAGE)
                .user("root")
                .workingDir("/data")
                .cmd("java -cp test-collection.jar io.pravega.test.system.SingleJUnitTestRunner " +
                        className + "#" + methodName + " > server.log 2>&1" )
                //.cmd("sh", "-c", "while :; do sleep 1; done")
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


