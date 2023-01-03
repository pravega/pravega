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
package io.pravega.cli.admin.controller;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.CLIConfig;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import static io.pravega.common.util.CertificateUtils.createTrustStore;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

/**
 * Base for any Controller-related commands.
 */
@Slf4j
public abstract class ControllerCommand extends AdminCommand {
    static final String COMPONENT = "controller";

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates a context for child classes consisting of a REST client to execute calls against the Controller.
     *
     * @return REST client.
     */
    protected Context createContext() {
        CLIConfig config = getCLIControllerConfig();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");

        ClientBuilder builder = ClientBuilder.newBuilder().withConfig(clientConfig);

        // If TLS parameters are configured, set them in client.
        if (config.isTlsEnabled()) {
            SSLContext tlsContext;
            try {
                KeyStore ks = createTrustStore(config.getTruststore());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ks);
                tlsContext = SSLContext.getInstance("TLS");
                tlsContext.init(null, tmf.getTrustManagers(), null);
            } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | KeyManagementException e) {
                String message = String.format("Encountered exception while trying to use the given truststore: %s", config.getTruststore());
                log.error(message, e);
                return null;
            }
            builder.sslContext(tlsContext);
        }
        Client client = builder.build();
        // If authorization parameters are configured, set them in the client.
        if (config.isAuthEnabled()) {
            HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(config.getUserName(),
                    config.getPassword());
            client = client.register(auth);
        }
        return new Context(client);
    }

    /**
     * Generic method to execute execute a request against the Controller and get the response.
     *
     * @param context Controller command context.
     * @param requestURI URI to execute the request against.
     * @return Response for the REST call.
     */
    String executeRESTCall(Context context, String requestURI) {
        Response response = getInvocationBuilder(context, requestURI).get();
        printResponseInfo(response);
        return response.readEntity(String.class);
    }

    /**
     * Generic method to execute a delete request against the Controller and get the response.
     *
     * @param context Controller command context.
     * @param requestURI URI to execute the request against.
     * @return Response for the REST call.
     */
    protected String executeDeleteRESTCall(Context context, String requestURI) {
        Response response = getInvocationBuilder(context, requestURI).delete();
        printResponseInfo(response);
        return response.readEntity(String.class);
    }

    /**
     * Method to get invocationBuilder to execute rest api.
     *
     * @param context       Controller command context.
     * @param requestURI    URI to execute the request against.
     * @return              InvocationBuilder object.
     */
    private Invocation.Builder getInvocationBuilder(final Context context, final String requestURI) {
        String resourceURL = getCLIControllerConfig().getControllerRestURI() + requestURI;
        WebTarget webTarget = context.client.target(resourceURL);
        return webTarget.request();
    }

    @VisibleForTesting
    void printResponseInfo(Response response) {
        if (OK.getStatusCode() == response.getStatus() || NO_CONTENT.getStatusCode() == response.getStatus()) {
            output("Successful REST request.");
        } else if (UNAUTHORIZED.getStatusCode() == response.getStatus()) {
            output("Unauthorized REST request. You may need to set the user/password correctly.");
        } else {
            output("The REST request was not successful: " + response.getStatus());
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    protected static class Context implements AutoCloseable {
        final Client client;

        @Override
        public void close() {
            this.client.close();
        }
    }
}
