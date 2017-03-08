/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import feign.Client;
import feign.Feign;
import feign.RequestInterceptor;
import feign.RequestLine;
import feign.Response;
import feign.auth.BasicAuthRequestInterceptor;
import feign.gson.GsonEncoder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.util.Collection;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * This class is used to handle the Authentication with the authentication-service.
 */
public class LoginClient {

    static final String MESOS_MASTER = getMesosMasterIP();
    public static final String MESOS_URL = String.format("https://%s", MESOS_MASTER);
    static final String LOGIN_URL = MESOS_URL + "/auth/v1";

    private static final String TOKEN_HEADER_NAME = "X-AUTH-TOKEN";

    /**
     * Fetch the token from the authentication service.
     *
     *  @param loginURL           Login Url.
     *  @param requestInterceptor Auth request interceptor for basic authentication.
     *  @return Auth token.
     */
    public static String getAuthToken(final String loginURL, final RequestInterceptor requestInterceptor) {

        Login client = Feign.builder().client(getClientHostVerificationDisabled())
                .encoder(new GsonEncoder())
                .requestInterceptor(requestInterceptor)
                .target(Login.class, loginURL);

        Response response = client.login();

        if (response.status() == OK.code()) {
            Collection<String> headers = response.headers().get(TOKEN_HEADER_NAME);
            return headers.toArray(new String[headers.size()])[0];
        } else {
            throw new TestFrameworkException(TestFrameworkException.Type.LoginFailed, "Exception while " +
                    "logging into the cluster. Authentication service returned the following error: "
                    + response);
        }
    }

    /**
     * Get a client with host verification disabled.
     *
     * @return feign.Client
     */
    public static Client.Default getClientHostVerificationDisabled() {
        return new Client.Default(TrustingSSLSocketFactory.get(), new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        });
    }

    public static RequestInterceptor getAuthenticationRequestInterceptor() {
        return new BasicAuthRequestInterceptor(getUsername(), getPassword());
    }

    private static String getMesosMasterIP() {
        return getConfig("masterIP", "Invalid Master IP");
    }

    private static String getUsername() {
        return getConfig("userName", "admin");
    }

    private static String getPassword() {
        return getConfig("password", "password");
    }

    //Get configuration from environment or system property.
    private static String getConfig(final String key, final String defaultValue) {
        return System.getenv().getOrDefault(key, System.getProperty(key, defaultValue));
    }

    private interface Login {
        @RequestLine("POST /login")
        Response login();
    }
}
