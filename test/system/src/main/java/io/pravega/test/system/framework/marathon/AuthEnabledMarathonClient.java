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
package io.pravega.test.system.framework.marathon;

import feign.Feign;
import feign.Logger;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Response;
import feign.RetryableException;
import feign.Retryer;
import feign.codec.ErrorDecoder;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import io.pravega.test.system.framework.LoginClient;
import java.util.Calendar;
import mesosphere.client.common.ModelUtils;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;
import mesosphere.marathon.client.MarathonException;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Marathon client with authentication enabled.
 */
public class AuthEnabledMarathonClient {

    private static final String ENDPOINT = LoginClient.MESOS_URL + "/marathon";
    private static final String LOGIN_URL = LoginClient.MESOS_URL + "/auth/v1";
    private static final String APPLICATION_JSON = "application/json";

    public static Marathon getClient() {
        return createMarathonClient();
    }

    static class MarathonHeadersInterceptor implements RequestInterceptor {
        @Override
        public void apply(RequestTemplate template) {
            template.header("Accept", APPLICATION_JSON);
            template.header("Content-Type", APPLICATION_JSON);
        }
    }

    static class MarathonErrorDecoder implements ErrorDecoder {
        @Override
        public Exception decode(String methodKey, Response response) {
            //Retry in-case marathon service returns 503 or 500
            if (response.status() == SERVICE_UNAVAILABLE.getStatusCode() || response.status() ==
                    INTERNAL_SERVER_ERROR.getStatusCode()) {
                //retry after 5 seconds.
                Calendar retryAfter = Calendar.getInstance();
                retryAfter.add(Calendar.SECOND, 5);

                return new RetryableException("Received response code: " + response.status(), retryAfter.getTime());
            } else {
                return new MarathonException(response.status(), response.reason());
            }
        }
    }

    private static Marathon createMarathonClient() {
        String token = LoginClient.getAuthToken(LOGIN_URL);
        return getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
    }

    private static Marathon getInstance(String endpoint, RequestInterceptor... interceptors) {
        Feign.Builder b = Feign.builder().client(LoginClient.getClientHostVerificationDisabled())
                .logger(new Logger.ErrorLogger())
                .logLevel(Logger.Level.BASIC)
                .encoder(new GsonEncoder(ModelUtils.GSON))
                .decoder(new GsonDecoder(ModelUtils.GSON))
                .errorDecoder(new MarathonErrorDecoder())
                //max wait period = 5 seconds ; max attempts = 5
                .retryer(new Retryer.Default(SECONDS.toMillis(1), SECONDS.toMillis(5), 5));
        if (interceptors != null) {
            b.requestInterceptors(asList(interceptors));
        }
        b.requestInterceptor(new MarathonHeadersInterceptor());
        return b.target(Marathon.class, endpoint);
    }
}
