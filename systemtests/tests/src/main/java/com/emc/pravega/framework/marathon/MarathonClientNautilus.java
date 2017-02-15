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
import feign.Feign;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Response;
import feign.codec.ErrorDecoder;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;
import mesosphere.marathon.client.utils.MarathonException;
import mesosphere.marathon.client.utils.ModelUtils;

import static com.emc.pravega.framework.NautilusLoginClient.MESOS_URL;
import static com.emc.pravega.framework.NautilusLoginClient.getAuthenticationRequestInterceptor;
import static com.emc.pravega.framework.NautilusLoginClient.getClientHostVerificationDisabled;
import static java.util.Arrays.asList;

/**
 * Marathon client with Nautilus authentication enabled.
 */
public class MarathonClientNautilus {

    private static final String ENDPOINT = MESOS_URL + "/marathon";
    private static final String LOGIN_URL = MESOS_URL + "/auth/v1";

    public static Marathon getClient() {
        return createMarathonClient();
    }

    static class MarathonHeadersInterceptor implements RequestInterceptor {
        @Override
        public void apply(RequestTemplate template) {
            template.header("Accept", "application/json");
            template.header("Content-Type", "application/json");
        }
    }

    static class MarathonErrorDecoder implements ErrorDecoder {
        @Override
        public Exception decode(String methodKey, Response response) {
            return new MarathonException(response.status(), response.reason());
        }
    }

    private static Marathon createMarathonClient() {
        String token = NautilusLoginClient.getAuthToken(LOGIN_URL, getAuthenticationRequestInterceptor());
        return getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
    }

    private static Marathon getInstance(String endpoint, RequestInterceptor... interceptors) {
        Feign.Builder b = Feign.builder().client(getClientHostVerificationDisabled())
                .encoder(new GsonEncoder(ModelUtils.GSON))
                .decoder(new GsonDecoder(ModelUtils.GSON))
                .errorDecoder(new MarathonErrorDecoder());
        if (interceptors != null) {
            b.requestInterceptors(asList(interceptors));
        }
        b.requestInterceptor(new MarathonHeadersInterceptor());
        return b.target(Marathon.class, endpoint);
    }
}
