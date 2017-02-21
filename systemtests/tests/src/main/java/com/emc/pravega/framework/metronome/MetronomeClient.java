/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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

import static com.emc.pravega.framework.LoginClient.getClientHostVerificationDisabled;
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
     *  @param endpoint URL of Metronome
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
     *  @param endpoint URL of Marathon
     *  @param token    token
     *  @return Metronome client
     */
    public static Metronome getInstanceWithTokenAuth(String endpoint, String token) {
        return getInstance(endpoint, new TokenAuthRequestInterceptor(token));
    }
}
