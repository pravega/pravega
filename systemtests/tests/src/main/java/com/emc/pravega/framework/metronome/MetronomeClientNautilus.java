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
import com.emc.pravega.framework.metronome.model.v1.Job;
import feign.auth.BasicAuthRequestInterceptor;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;

import java.util.List;

import static com.emc.pravega.framework.NautilusLoginClient.MESOS_URL;

/**
 * Metronome client with Nautilus authentication enabled.
 */
public class MetronomeClientNautilus {

    private static final String ENDPOINT = MESOS_URL + "/service/metronome";
    private static final String LOGIN_URL = MESOS_URL + "/auth/v1";

    public static Metronome getClient() {
        return createMetronomeClient();
    }

    private static Metronome createMetronomeClient() {
        //TODO: Remove hardcoding for username and password.
        final BasicAuthRequestInterceptor requestInterceptor = new BasicAuthRequestInterceptor("admin", "password");
        String token = NautilusLoginClient.getAuthToken(LOGIN_URL, requestInterceptor);
        return MetronomeClient.getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
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
}
