/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.metronome;

import io.pravega.test.system.framework.LoginClient;
import io.pravega.test.system.framework.metronome.model.v1.Job;
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
        String token = LoginClient.getAuthToken(LOGIN_URL);
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
