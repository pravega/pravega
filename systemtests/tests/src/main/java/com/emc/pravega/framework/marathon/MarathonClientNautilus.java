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

import feign.Feign;
import feign.RequestLine;
import feign.Response;
import feign.auth.BasicAuthRequestInterceptor;
import feign.gson.GsonEncoder;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;

import java.util.Collection;

public class MarathonClientNautilus {

    public static final String TOKEN_HEADER_NAME = "X-AUTH-TOKEN";

    //TODO: Read this from system properties
    private static final String ENDPOINT = "http://10.240.124.4/marathon";
    private static final String LOGIN_URL = "http://10.240.124.4/auth/v1";

    public static Marathon getClient() {
        return createMarathonClient();
    }

    private interface LoginClient {
        @RequestLine("POST /login")
        Response login();
    }

    private static Marathon createMarathonClient() {
        final BasicAuthRequestInterceptor requestInterceptor = new BasicAuthRequestInterceptor("admin", "password");
        String token = getAuthToken(requestInterceptor);
        return MarathonClient.getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
    }

    private static String getAuthToken(BasicAuthRequestInterceptor requestInterceptor) {
        LoginClient client = Feign.builder()
                .encoder(new GsonEncoder())
                .requestInterceptor(requestInterceptor)
                .target(LoginClient.class, LOGIN_URL);

        Response response = client.login();

        if (response.status() == 200) {
            Collection<String> headers = response.headers().get(TOKEN_HEADER_NAME);
            return headers.toArray(new String[headers.size()])[0];
        } else {
            throw new RuntimeException("Exception while logging into the nautilus cluster");
        }
    }
}
