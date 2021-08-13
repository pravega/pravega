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

import feign.Response;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.metronome.AuthEnabledMetronomeClient;
import io.pravega.test.system.framework.metronome.Metronome;
import io.pravega.test.system.framework.metronome.MetronomeException;
import io.pravega.test.system.framework.metronome.model.v1.Artifact;
import io.pravega.test.system.framework.metronome.model.v1.Job;
import io.pravega.test.system.framework.metronome.model.v1.Restart;
import io.pravega.test.system.framework.metronome.model.v1.Run;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import static javax.ws.rs.core.Response.Status.CREATED;

/**
 * Remote Sequential is TestExecutor which runs the test as Mesos Task.
 * This is used to execute tests sequentially.
 */
@Slf4j
public class RemoteSequential implements TestExecutor {
    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(3, "test");

    @Override
    public CompletableFuture<Void> startTestExecution(Method testMethod) {
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(60));
        // This will be removed once issue https://github.com/pravega/pravega/issues/1665 is resolved.

        log.debug("Starting test execution for method: {}", testMethod);
        final Metronome client = AuthEnabledMetronomeClient.getClient();

        String className = testMethod.getDeclaringClass().getName();
        String methodName = testMethod.getName();
        String jobId = (methodName + ".testJob").toLowerCase(); //All jobIds should have lowercase for metronome.

        return CompletableFuture.runAsync(() -> {
            client.createJob(newJob(jobId, className, methodName));
            Response response = client.triggerJobRun(jobId);
            if (response.status() != CREATED.getStatusCode()) {
                throw new TestFrameworkException(TestFrameworkException.Type.ConnectionFailed, "Error while starting " +
                        "test " + testMethod);
            } else {
                log.info("Created job succeeded with: " + response.toString());
            }
        }).thenCompose(v2 -> waitForJobCompletion(jobId, client))
                .<Void>thenApply(v1 -> {
                    if (client.getJob(jobId).getHistory().getFailureCount() != 0) {
                        throw new AssertionError("Test failed, detailed logs can be found at " +
                                "https://MasterIP/mesos, under metronome framework tasks. MethodName: " + methodName);
                    }
                    return null;
                }).whenComplete((v, ex) -> {
                    deleteJob(jobId, client); //deletejob once execution is complete.
                    if (ex != null) {
                        log.error("Error while executing the test. ClassName: {}, MethodName: {}", className,
                                methodName);

                    }
                });
    }

    @Override
    public void stopTestExecution() {
        throw new NotImplementedException("Stop Execution is not used for Remote sequential execution");
    }

    private CompletableFuture<Void> waitForJobCompletion(final String jobId, final Metronome client) {
        return Futures.loop(() -> isTestRunning(jobId, client),
                () -> Futures.delayedFuture(Duration.ofSeconds(10), executorService),
                executorService);
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
        art.setUri(System.getProperty("testArtifactUrl", "InvalidTestArtifactURL"));

        Restart restart = new Restart();
        restart.setActiveDeadlineSeconds(120); // the tests are expected to finish in 2 mins, this can be changed to
        // a higher value if required.
        restart.setPolicy("NEVER");

        Run run = new Run();
        run.setArtifacts(Collections.singletonList(art));

        run.setCmd("docker run --rm -v $(pwd):/data " + System.getProperty("dockerImageRegistry") + "/java:8 java" +
                " -DmasterIP=" + LoginClient.MESOS_MASTER +
                " -DskipServiceInstallation=" + Utils.isSkipServiceInstallationEnabled() +
                " -cp /data/pravega-test-system-" + System.getProperty("testVersion") + ".jar io.pravega.test.system.SingleJUnitTestRunner " +
                className + "#" + methodName + " > server.log 2>&1" +
                "; exit $?");

        run.setCpus(0.5); //CPU shares.
        run.setMem(512.0); //amount of memory required for running test in MB.
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

    private boolean isTestRunning(final String jobId, final Metronome client) {
        Job jobStatus = client.getJob(jobId);
        boolean isRunning = false;
        if (jobStatus.getHistory() == null) {
            isRunning = true;
        } else if ((jobStatus.getHistory().getSuccessCount() == 0) && (jobStatus.getHistory().getFailureCount() == 0)) {
            isRunning = true;
        }
        return isRunning;
    }

    private void deleteJob(final String jobId, final Metronome client) {
        try {
            client.deleteJob(jobId);
        } catch (MetronomeException e) {
            throw new TestFrameworkException(TestFrameworkException.Type.RequestFailed, "Error while deleting the " +
                    "test run job", e);
        }
    }
}
