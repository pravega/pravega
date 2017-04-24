/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.framework.metronome;

import io.pravega.framework.LoginClient;
import io.pravega.framework.metronome.model.v1.Job;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;

import java.util.List;

/**
 * Metronome client with authentication enabled.
 */
@Slf4j
public class AuthEnabledMetronomeClient {

    private static final String ENDPOINT = LoginClient.MESOS_URL + "/service/metronome";
    private static final String LOGIN_URL = LoginClient.MESOS_URL + "/auth/v1";

    public static Metronome getClient() {
        return createMetronomeClient();
    }

    private static Metronome createMetronomeClient() {
        String token = LoginClient.getAuthToken(LOGIN_URL, LoginClient.getAuthenticationRequestInterceptor());
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
