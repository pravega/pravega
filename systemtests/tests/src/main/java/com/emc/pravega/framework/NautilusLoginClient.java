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

import feign.Feign;
import feign.RequestLine;
import feign.Response;
import feign.auth.BasicAuthRequestInterceptor;
import feign.gson.GsonEncoder;

import java.util.Collection;

public class NautilusLoginClient {

    public static final String MESOS_MASTER = System.getProperty("masterIP", "10.240.124.139");
    public static final String MESOS_URL = String.format("http://%s", MESOS_MASTER);

    static final String TOKEN_HEADER_NAME = "X-AUTH-TOKEN";

    private interface Login {
        @RequestLine("POST /login")
        Response login();
    }

    public static String getAuthToken(final String loginURL, final BasicAuthRequestInterceptor requestInterceptor) {
        Login client = Feign.builder()
                .encoder(new GsonEncoder())
                .requestInterceptor(requestInterceptor)
                .target(Login.class, loginURL);

        Response response = client.login();

        if (response.status() == 200) {
            Collection<String> headers = response.headers().get(TOKEN_HEADER_NAME);
            return headers.toArray(new String[headers.size()])[0];
        } else {
            throw new RuntimeException("Exception while logging into the nautilus cluster. Nautilus Authentication" +
                    "service returned an error" + response);
        }
    }
}
