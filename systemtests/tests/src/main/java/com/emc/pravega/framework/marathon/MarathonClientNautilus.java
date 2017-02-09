/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.framework.marathon;

import com.emc.pravega.framework.NautilusLoginClient;
import feign.auth.BasicAuthRequestInterceptor;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;

import static com.emc.pravega.framework.NautilusLoginClient.MESOS_URL;

/**
 * Marathon client with Nautilus authentication enabled.
 */
public class MarathonClientNautilus {

    private static final String ENDPOINT = MESOS_URL + "/marathon";
    private static final String LOGIN_URL = MESOS_URL + "/auth/v1";

    public static Marathon getClient() {
        return createMarathonClient();
    }

    private static Marathon createMarathonClient() {
        //TODO: Remove username and password hardcoding.
        final BasicAuthRequestInterceptor requestInterceptor = new BasicAuthRequestInterceptor("admin", "password");
        String token = NautilusLoginClient.getAuthToken(LOGIN_URL, requestInterceptor);
        return MarathonClient.getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
    }
}
