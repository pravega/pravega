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

import com.emc.pravega.framework.metronome.Metronome;
import com.emc.pravega.framework.metronome.MetronomeClientNautilus;
import com.emc.pravega.framework.metronome.MetronomeException;
import com.emc.pravega.framework.metronome.model.v1.Artifact;
import com.emc.pravega.framework.metronome.model.v1.Job;
import com.emc.pravega.framework.metronome.model.v1.Restart;
import com.emc.pravega.framework.metronome.model.v1.Run;
import feign.Response;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RemoteSequential implements TestExecutor {
    private static Metronome client = MetronomeClientNautilus.getClient();

    @Override
    public CompletableFuture<String> startTestExecution(Method method) {
        System.out.println(method);
        String className = method.getDeclaringClass().getName();
        String methodName = method.getName();
        String jobId = (methodName + ".testJob.01").toLowerCase();
        try {
            Job job = client.createJob(newJob(jobId, className, methodName));
            Response response = client.triggerJobRun(jobId);
            if (response.status() != 201) {
                throw new RuntimeException("Error while executing test" + method.toString());
            }

            //wait until the test is complete.
            while (isTestRunning(jobId)) {
                TimeUnit.SECONDS.sleep(3);
            }
            if (client.getJob(jobId).getHistory().getFailureCount() != 0) {
                throw new AssertionError("Error while executing Test" + methodName);
            }

        } catch (MetronomeException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            deleteJob(jobId);
        }
        return CompletableFuture.completedFuture("completed");
    }

    @Override
    public CompletableFuture<String> stopTestExcecution(String testID) {
        return null;
    }

    private static Job newJob(String id, String className, String methodName) {
        Map<String, String> labels = new HashMap<>(1);
        labels.put("label1", "value1");

        Map<String, String> env = new HashMap<>(2);
        env.put("env1", "value101");
        env.put("env2", "value102");

        Artifact art = new Artifact();
        art.setCache(false);
        art.setExecutable(false);
        art.setExtract(false);
        art.setUri("http://asdrepo.isus.emc.com:8081/artifactory/pravega-testframework/pravega/systemtests/0.2" +
                "/systemtests-0.2.jar");

        Restart restart = new Restart();
        restart.setActiveDeadlineSeconds(120);
        restart.setPolicy("NEVER");

        Run r = new Run();
        r.setArtifacts(Collections.singletonList(art));

        r.setCmd("docker run --rm --name=\"testCase-1\" -v $(pwd):/data cogniteev/oracle-java:latest java -cp " +
                "/data/systemtests-0.2.jar com.emc.pravega.SingleJUnitTestRunner " +
                className + "#" + methodName + " > server.log 2>&1" +
                "; exit $?");
        //        r.setCmd("docker run --lrm -it --name=\\\"testCase\\\" -v $(pwd):/data cogniteev/oracle-java:latest
        // java
        // -cp " +
        //                "/data/systemtests-0.1.jar com.emc.pravega.SingleJUnitTestRunner " +
        //                className + "#" + methodName +
        //                "; echo \"Testingmmmmmmmmmmmm\" ; exit $?");
        r.setCpus(0.5);
        r.setMem(64.0);
        r.setDisk(50.0);
        r.setEnv(env);
        r.setMaxLaunchDelay(3600);
        r.setRestart(restart);
        r.setUser("root");

        Job j = new Job();
        j.setId(id);
        j.setDescription("job-1 first ");
        j.setLabels(labels);
        j.setRun(r);

        return j;
    }

    private boolean isTestRunning(String jobId) throws MetronomeException {
        Job jobStatus = client.getJob(jobId);
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
            client.deleteJob(jobId);
        } catch (MetronomeException e) {
            e.printStackTrace();
        }
    }
}

