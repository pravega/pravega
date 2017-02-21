/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
