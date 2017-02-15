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
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;

import java.util.List;

import static com.emc.pravega.framework.NautilusLoginClient.MESOS_URL;
import static com.emc.pravega.framework.NautilusLoginClient.getAuthenticationRequestInterceptor;

/**
 * Metronome client with Nautilus authentication enabled.
 */
@Slf4j
public class MetronomeClientNautilus {

    private static final String ENDPOINT = MESOS_URL + "/service/metronome";
    private static final String LOGIN_URL = MESOS_URL + "/auth/v1";

    public static Metronome getClient() {
        return createMetronomeClient();
    }

    private static Metronome createMetronomeClient() {
        String token = NautilusLoginClient.getAuthToken(LOGIN_URL, getAuthenticationRequestInterceptor());
        return MetronomeClient.getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
    }

    public static void deleteAllJobs(Metronome client) throws MetronomeException {
        List<Job> list = client.getJobs();
        list.forEach(job -> {
            try {
                client.deleteJob(job.getId());
            } catch (MetronomeException e) {
                log.error("Exception while deleting Metronome jobs", e);
            }
        });
    }
}
