/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework.metronome;

import feign.Feign;
import feign.Logger;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Response;
import feign.RetryableException;
import feign.Retryer;
import feign.auth.BasicAuthRequestInterceptor;
import feign.codec.ErrorDecoder;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import io.pravega.test.system.framework.LoginClient;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;
import mesosphere.client.common.ModelUtils;
import java.util.Calendar;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MetronomeClient {
    private static class MetronomeHeadersInterceptor implements RequestInterceptor {
        @Override
        public void apply(RequestTemplate template) {
            template.header("Accept", "application/json");
        }
    }

    static class MetronomeErrorDecoder implements ErrorDecoder {
        @Override
        public Exception decode(String methodKey, Response response) {
            //Retry in case Metronome service returns 503 or 500
            if (response.status() == SERVICE_UNAVAILABLE.getStatusCode() || response.status() ==
                    INTERNAL_SERVER_ERROR.getStatusCode()) {
                //retry after 5 seconds.
                Calendar retryAfter = Calendar.getInstance();
                retryAfter.add(Calendar.SECOND, 5);

                return new RetryableException("Received response code: " + response.status(), retryAfter.getTime());
            } else {
                return new MetronomeException(response.status(), response.reason());
            }
        }
    }

    public static Metronome getInstance(String endpoint) {
        return getInstance(endpoint, (RequestInterceptor) null);
    }

    /*
     * The generalized version of the method that allows more in-depth customizations via
     * {@link RequestInterceptor}s.
     *
     *  @param endpoint URL of Metronome
     */
    public static Metronome getInstance(String endpoint, RequestInterceptor... interceptors) {
        Feign.Builder b = Feign.builder().client(LoginClient.getClientHostVerificationDisabled())
                .logger(new Logger.ErrorLogger())
                .logLevel(Logger.Level.BASIC)
                .encoder(new GsonEncoder(ModelUtils.GSON))
                .decoder(new GsonDecoder(ModelUtils.GSON))
                //max wait period = 5 seconds ; max attempts = 5
                .retryer(new Retryer.Default(SECONDS.toMillis(1), SECONDS.toMillis(5), 5))
                .errorDecoder(new MetronomeErrorDecoder());
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
     *  @param endpoint URL of Marathon
     *  @param token    token
     *  @return Metronome client
     */
    public static Metronome getInstanceWithTokenAuth(String endpoint, String token) {
        return getInstance(endpoint, new TokenAuthRequestInterceptor(token));
    }
}
