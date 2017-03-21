/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.framework.marathon;

import com.emc.pravega.framework.LoginClient;
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
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.auth.TokenAuthRequestInterceptor;
import mesosphere.marathon.client.utils.MarathonException;
import mesosphere.marathon.client.utils.ModelUtils;

import java.util.Calendar;

import static com.emc.pravega.framework.LoginClient.MESOS_URL;
import static com.emc.pravega.framework.LoginClient.getAuthenticationRequestInterceptor;
import static com.emc.pravega.framework.LoginClient.getClientHostVerificationDisabled;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Marathon client with authentication enabled.
 */
public class AuthEnabledMarathonClient {

    private static final String ENDPOINT = MESOS_URL + "/marathon";
    private static final String LOGIN_URL = MESOS_URL + "/auth/v1";
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
            if (response.status() == SERVICE_UNAVAILABLE.code() || response.status() ==
                    INTERNAL_SERVER_ERROR.code()) {
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
        String token = LoginClient.getAuthToken(LOGIN_URL, getAuthenticationRequestInterceptor());
        return getInstance(ENDPOINT, new TokenAuthRequestInterceptor(token));
    }

    private static Marathon getInstance(String endpoint, RequestInterceptor... interceptors) {
        Feign.Builder b = Feign.builder().client(getClientHostVerificationDisabled())
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
