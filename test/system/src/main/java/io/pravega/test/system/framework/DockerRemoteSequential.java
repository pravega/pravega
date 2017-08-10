/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import io.pravega.common.concurrent.FutureHelpers;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

@Slf4j
public class DockerRemoteSequential implements TestExecutor {

    private static final String MASTER_IP = System.getProperty("masterIP");
    private static final DockerClient CLIENT = DefaultDockerClient.builder().uri(MASTER_IP).build();
    private static final String IMAGE = "java:8";
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    @Override
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
        throw new NotImplementedException("Stop Execution is not used for Remote sequential execution");
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
            assertThat(containerCreation.id(), is(notNullValue()));

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

        String env = "masterIP=" + MASTER_IP;
        List<String> stringList = new ArrayList<>();
        stringList.add(env);

        List<String> cmdList = new ArrayList<>();
        String cmd = "cp /data/test/system/build/libs/test-collection.jar io.pravega.test.system.SingleJUnitTestRunner " +
                className + "#" + methodName + " > server.log 2>&1" +
                "; exit $?";
        cmdList.add(cmd);

        HostConfig hostConfig = HostConfig.builder().build();

        String volume = "$(pwd):/data";

        ContainerConfig containerConfig = ContainerConfig.builder()
                .addVolume(volume)
                .hostConfig(hostConfig)
                .image(IMAGE)
                .cmd(cmdList)
                .labels(labels)
                .env(env)
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
