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
package io.pravega.test.system.framework;

import feign.Client;
import feign.Feign;
import feign.Headers;
import feign.RequestInterceptor;
import feign.RequestLine;
import feign.Response;
import feign.auth.BasicAuthRequestInterceptor;
import feign.gson.GsonEncoder;
import java.util.Collection;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import lombok.Data;
import mesosphere.client.common.ModelUtils;

import static javax.ws.rs.core.Response.Status.OK;
import static io.pravega.test.system.framework.Utils.getConfig;

/**
 * This class is used to handle the Authentication with the authentication-service.
 */
public class LoginClient {

    static final String MESOS_MASTER = getMesosMasterIP();
    public static final String MESOS_URL = String.format("https://%s", MESOS_MASTER);

    private static final String TOKEN_HEADER_NAME = "X-AUTH-TOKEN";

    /**
     * Fetch the token from the authentication service.
     *
     *  @param loginURL           Login Url.
     *  @return Auth token.
     */
    public static String getAuthToken(final String loginURL) {

        Login client = Feign.builder().client(getClientHostVerificationDisabled())
                .encoder(new GsonEncoder(ModelUtils.GSON))
                .target(Login.class, loginURL);

        Response response = client.login(new AuthRequest(getUsername(), getPassword(), "LOCAL"));

        if (response.status() == OK.getStatusCode()) {
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
        return  Utils.isAwsExecution() ? getConfig("awsMasterIP", "Invalid Master IP").trim() : getConfig("masterIP", "Invalid Master IP");
    }

    private static String getUsername() {
        return getConfig("userName", "admin");
    }

    private static String getPassword() {
        return getConfig("password", "password");
    }

    private interface Login {
        @RequestLine("POST /login")
        @Headers("Content-Type: application/json")
        Response login(AuthRequest auth);
    }

    @Data
    private static class AuthRequest {
        private final String username;
        private final String password;
        private final String idpId;
    }
}
