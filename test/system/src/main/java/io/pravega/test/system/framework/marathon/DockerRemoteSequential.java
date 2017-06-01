/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.marathon;


import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.test.system.framework.TestExecutor;
import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.metronome.MetronomeException;
import io.pravega.test.system.framework.metronome.model.v1.Job;
import org.apache.commons.lang.NotImplementedException;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
public class DockerRemoteSequential implements TestExecutor{

    private static final DockerClient CLIENT = DefaultDockerClient.builder().uri("http://localhost:2375").build();
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    @Override
    public CompletableFuture<Void> startTestExecution(Method testMethod) {

        log.debug("Starting test execution for method: {}", testMethod);

        String className = testMethod.getDeclaringClass().getName();
        String methodName = testMethod.getName();
        //String jobId = (methodName + ".testJob").toLowerCase(); //All jobIds should have lowercase for metronome.

    }

    @Override
    public CompletableFuture<Void> stopTestExecution(String testID) {
        throw new NotImplementedException("Stop Execution is not used for Remote sequential execution");
    }


    private CompletableFuture<Void> waitForJobCompletion(final String jobId) {
        return FutureHelpers.loop(() -> isTestRunning(jobId),
                () -> FutureHelpers.delayedFuture(Duration.ofSeconds(3), executorService),
                executorService);
    }
    private boolean isTestRunning(String jobId) {
        Job jobStatus = CLIENT.getJob(jobId);
        boolean isRunning = false;
        if (jobStatus.getHistory() == null) {
            isRunning = true;
        } else if ((jobStatus.getHistory().getSuccessCount() == 0) && (jobStatus.getHistory().getFailureCount() == 0)) {
            isRunning = true;
        }
        return isRunning;
    }

    private void deleteApp(String jobId) {
        try {
            CLIENT.deleteJob(jobId);
        } catch (MetronomeException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Error while deleting the " +
                    "test run job", e);
        }
    }
}
