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
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import io.pravega.common.concurrent.FutureHelpers;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static io.pravega.test.system.framework.Utils.getConfig;
import static org.junit.Assert.assertFalse;

@Slf4j
public class DockerRemoteSequential implements TestExecutor {

    public final static DockerClient CLIENT = DefaultDockerClient.builder().uri("http://"+ getConfig("masterIP", "Invalid Master IP")+ ":2375").build();
    private final static String IMAGE = "java:8";
    public String id;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    public CompletableFuture<Void> startTestExecution(Method testMethod) {

        log.debug("Starting test execution for method: {}", testMethod);

        String className = testMethod.getDeclaringClass().getName();
        String methodName = testMethod.getName();
        String containerName = methodName + ".testJob";

        return CompletableFuture.runAsync(() -> {
            startTest(containerName, className, methodName);
        }).thenCompose(v2 -> waitForJobCompletion(containerName))
                .whenComplete((v, ex) -> {
            stopContainer(); //stop container once execution is complete.
                    if (ex != null) {
                        log.error("Error while executing the test. ClassName: {}, MethodName: {}", className,
                                methodName);
                    }
                }
                );
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
            ContainerInfo info = CLIENT.inspectContainer(id);
                if (info.name().equals(containerName)) {
                    value = true;
                }
        } catch (DockerException | InterruptedException e) {
            log.error("unable to list docker services ", e);
        }
        return value;
    }

    private String startTest(String containerName, String className, String methodName) {

        try {
            CLIENT.pull(IMAGE);

            ContainerCreation containerCreation = CLIENT.createContainer(setContainerConfig(methodName, className), containerName);
            assertFalse(containerCreation.id().toString().equals(null));

              id = containerCreation.id();

            final Path dockerDirectory = Paths.get(System.getProperty("user.dir"));
            try {
                CLIENT.copyToContainer(dockerDirectory, id, "/data");
            } catch (IOException | DockerException | InterruptedException e) {
                log.error("Exception while copying test jar to the container ", e);
                Assert.fail("Unable to copy test jar to the container.Test failure");
            }

            // Inspect container
            final ContainerInfo info = CLIENT.inspectContainer(id);

            // Start container
            CLIENT.startContainer(id);

        } catch (DockerException | InterruptedException e) {
            log.error("exception in starting container ", e);
            Assert.fail("Unable to start the container to invoke the test.Test failure");
        }
        return id;
    }

    private ContainerConfig setContainerConfig(String methodName, String className) {

        Map<String, String> labels = new HashMap<>(1);
        labels.put("testMethodName", methodName);
        labels.put("testClassName", className);

        HostConfig hostConfig = HostConfig.builder()
                .portBindings(ImmutableMap.of( "2375/tcp", Arrays.asList( PortBinding.of( System.getProperty("masterIP"), "2375" ) ) ) ).networkMode("host").build();

        ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image(IMAGE)
                .user("root")
                .workingDir("/data")
                .cmd("sh", "-c", "java -DmasterIP="+System.getProperty("masterIP")+ " -cp /data/build/libs/test-collection.jar io.pravega.test.system.SingleJUnitTestRunner " +
                        className + "#" + methodName + " > "+  className + "#" + methodName + "server.log 2>&1" + "; exit $?")
                .labels(labels)
                .build();

        return containerConfig;
    }

    private void stopContainer() {
        try {
            CLIENT.stopContainer(id, 0);
        } catch (DockerException | InterruptedException e) {
            log.error("Exception while stopping the container ", e);
        }
      }
}
