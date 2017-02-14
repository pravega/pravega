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

import feign.Feign;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Response;
import feign.auth.BasicAuthRequestInterceptor;
import feign.codec.ErrorDecoder;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;

import static com.emc.pravega.framework.NautilusLoginClient.getClientHostVerificationDisabled;
import static java.util.Arrays.asList;

public class MetronomeClient {
    private static class MetronomeHeadersInterceptor implements RequestInterceptor {
        @Override
        public void apply(RequestTemplate template) {
            template.header("Accept", "application/json");
        }
    }

    private static class MetronomeErrorDecoder implements ErrorDecoder {
        @Override
        public Exception decode(String methodKey, Response response) {
            return new MetronomeException(response.status(), response.reason());
        }
    }

    public static Metronome getInstance(String endpoint) {
        return getInstance(endpoint, (RequestInterceptor) null);
    }

    /*
     * The generalized version of the method that allows more in-depth customizations via
     * {@link RequestInterceptor}s.
     *
     * @param endpoint URL of Metronome
     */
    public static Metronome getInstance(String endpoint, RequestInterceptor... interceptors) {
        Feign.Builder b = Feign.builder().client(getClientHostVerificationDisabled())
                .encoder(new GsonEncoder(mesosphere.marathon.client.utils.ModelUtils.GSON))
                .decoder(new GsonDecoder(mesosphere.marathon.client.utils.ModelUtils.GSON))
                .errorDecoder(new MetronomeClient.MetronomeErrorDecoder());
        if (interceptors != null) {
            b.requestInterceptors(asList(interceptors));
        }
        b.requestInterceptor(new MetronomeHeadersInterceptor());
        return b.target(Metronome.class, endpoint);
    }

    /*
     * Creates a Metronome client proxy that performs HTTP basic authentication.
     */
    public static Metronome getInstanceWithBasicAuth(String endpoint, String username, String password) {
        return getInstance(endpoint, new BasicAuthRequestInterceptor(username, password));
    }

    /*
     * Creates a Marathon client proxy that uses a token for authentication.
     *
     * @param endpoint URL of Marathon
     * @param token    token
     * @return Metronome client
     */
    public static Metronome getInstanceWithTokenAuth(String endpoint, String token) {
        return getInstance(endpoint, new TokenAuthRequestInterceptor(token));
    }
}
