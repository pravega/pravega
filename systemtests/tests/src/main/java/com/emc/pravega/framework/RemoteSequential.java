/**
 * Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.framework;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.framework.metronome.Metronome;
import com.emc.pravega.framework.metronome.MetronomeClientNautilus;
import com.emc.pravega.framework.metronome.MetronomeException;
import com.emc.pravega.framework.metronome.model.v1.Artifact;
import com.emc.pravega.framework.metronome.model.v1.Job;
import com.emc.pravega.framework.metronome.model.v1.Restart;
import com.emc.pravega.framework.metronome.model.v1.Run;
import feign.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.emc.pravega.framework.NautilusLoginClient.MESOS_MASTER;
import static javax.ws.rs.core.Response.Status.CREATED;

/**
 * Remote Sequential is TestExecutor which runs the test as Mesos Task.
 * This is used to execute tests sequentially.
 */
@Slf4j
public class RemoteSequential implements TestExecutor {
    private static final Metronome CLIENT = MetronomeClientNautilus.getClient();
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    @Override
    public CompletableFuture<Void> startTestExecution(Method method) {

        log.debug("Starting test execution for method: {}", method);

        String className = method.getDeclaringClass().getName();
        String methodName = method.getName();
        String jobId = (methodName + ".testJob").toLowerCase(); //All jobIds should have lowercase for metronome.

        return CompletableFuture.<Void>runAsync(() -> {
            CLIENT.createJob(newJob(jobId, className, methodName));
            Response response = CLIENT.triggerJobRun(jobId);
            if (response.status() != CREATED.getStatusCode()) {
                throw new TestFrameworkException(TestFrameworkException.Type.ConnectionFailed, "Error while starting " +
                        "test " + method);
            }
        }).thenCompose(v2 -> waitForJobCompletion(jobId))
                .<Void>thenApply(v1 -> {
                    if (CLIENT.getJob(jobId).getHistory().getFailureCount() != 0) {
                        throw new AssertionError("Test failed, detailed logs can be found at " +
                                "https://MasterIP/mesos, under metronome framework tasks. MethodName: " + methodName);
                    }
                    return null;
                }).whenComplete((v, ex) -> {
                    deleteJob(jobId); //deletejob once execution is complete.
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

    private CompletableFuture<Void> waitForJobCompletion(final String jobId) {
        AtomicBoolean mustWait = new AtomicBoolean(true);

        return FutureHelpers.loop(mustWait::get, //condition
                () -> CompletableFuture.runAsync(() -> {  //loop body
                    mustWait.set(isTestRunning(jobId));
                }).thenCompose(v ->
                        FutureHelpers.delayedFuture(mustWait.get() ? Duration.ofSeconds(3) : Duration.ZERO,
                                executorService)
                ), executorService);
    }

    private Job newJob(String id, String className, String methodName) {
        Map<String, String> labels = new HashMap<>(1);
        labels.put("testMethodName", methodName);

        //This can be used to set environment variables while executing the job on Metronome.
        Map<String, String> env = new HashMap<>(2);
        env.put("masterIP", System.getProperty("masterIP"));
        env.put("env2", "value102");

        Artifact art = new Artifact();
        art.setCache(false); // It caches the artifacts, disabling it for now.
        art.setExecutable(false); // jar is not executable.
        art.setExtract(false);
        art.setUri("http://asdrepo.isus.emc.com:8081/artifactory/nautilus-pravega-testframework/pravega/systemtests" +
                "/0.1/systemtests-0.1.jar");

        Restart restart = new Restart();
        restart.setActiveDeadlineSeconds(120); // the tests are expected to finish in 2 mins, this can be changed to
        // a higher value if required.
        restart.setPolicy("NEVER");

        Run run = new Run();
        run.setArtifacts(Collections.singletonList(art));

        run.setCmd("docker run --rm --name=\"testCase-1\" -v $(pwd):/data asdrepo.isus.emc.com:8103/java:8 java" +
                " -DmasterIP=" + MESOS_MASTER +
                " -cp /data/systemtests-0.1.jar com.emc.pravega.SingleJUnitTestRunner " +
                className + "#" + methodName + " > server.log 2>&1" +
                "; exit $?");

        run.setCpus(0.5);
        run.setMem(64.0);
        run.setDisk(50.0);
        run.setEnv(env);
        run.setMaxLaunchDelay(3600);
        run.setRestart(restart);
        run.setUser("root");

        Job job = new Job();
        job.setId(id);
        job.setDescription(id);
        job.setLabels(labels);
        job.setRun(run);

        return job;
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

    private void deleteJob(String jobId) {
        try {
            CLIENT.deleteJob(jobId);
        } catch (MetronomeException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Error while deleting the " +
                    "test run job", e);
        }
    }
}
