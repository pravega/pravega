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

package com.emc.pravega.framework.metronome;

import com.emc.pravega.framework.NautilusLoginClient;
import com.emc.pravega.framework.metronome.model.v1.Artifact;
import com.emc.pravega.framework.metronome.model.v1.Job;
import com.emc.pravega.framework.metronome.model.v1.Restart;
import com.emc.pravega.framework.metronome.model.v1.Run;
import feign.auth.BasicAuthRequestInterceptor;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.emc.pravega.framework.NautilusLoginClient.MESOS_URL;

public class MetronomeClientNautilus {

    private static final String TOKEN_HEADER_NAME = "X-AUTH-TOKEN";

    //TODO: Read this from system properties
    private static final String ENDPOINT = MESOS_URL + "/service/metronome";
    private static final String LOGIN_URL = MESOS_URL + "/auth/v1";

    public static Metronome getClient() {
        return createMetronomeClient();
    }


    private static Metronome createMetronomeClient() {
        final BasicAuthRequestInterceptor requestInterceptor = new BasicAuthRequestInterceptor("admin", "password");
        String token = NautilusLoginClient.getAuthToken(LOGIN_URL, requestInterceptor);
        return MetronomeClient.getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
    }

    public static void main(String[] args) throws MetronomeException, InterruptedException {

        Metronome client = createMetronomeClient();
        deleteAllJobs(client);

        //        final String id = "learn-11";
        //        client.createJob(newJob(id, "com.emc.pravega.RedisClientTest", "redisPingTest"));
        //        client.triggerJobRun(id);
        //        TimeUnit.SECONDS.sleep(3);
        //        Job jobDetails = client.getJob(id);
        //        //        TimeUnit.SECONDS.sleep(20);
        //        System.out.println(jobDetails);
        //        System.out.println("hw");

    }

    private static void deleteAllJobs(Metronome client) throws MetronomeException {
        List<Job> list = client.getJobs();
        list.forEach(job -> {
            try {
                client.deleteJob(job.getId());
            } catch (MetronomeException e) {
                e.printStackTrace();
            }
        });
    }

    private static Job newJob(String id, String className, String methodName) {
        Map<String, String> labels = new HashMap<>(1);
        labels.put("label1", "value1");

        Map<String, String> env = new HashMap<>(2);
        env.put("env1", "value101");
        env.put("env2", "value102");

        Artifact art = new Artifact();
        art.setCache(true);
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
}
