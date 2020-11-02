/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.controller;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

/**
 * Base for any Controller-related commands.
 */
public abstract class ControllerCommand extends AdminCommand {
    static final String COMPONENT = "controller";

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    ControllerCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates a context for child classes consisting of a REST client to execute calls against the Controller.
     *
     * @return REST client.
     */
    protected Context createContext() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);
        // If authorization parameters are configured, set them in the client.
        if (getCLIControllerConfig().isAuthEnabled()) {
            HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(getCLIControllerConfig().getUserName(),
                    getCLIControllerConfig().getPassword());
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
        Invocation.Builder builder;
        String resourceURL = getCLIControllerConfig().getControllerRestURI() + requestURI;
        WebTarget webTarget = context.client.target(resourceURL);
        builder = webTarget.request();
        Response response = builder.get();
        printResponseInfo(response);
        return response.readEntity(String.class);
    }

    @VisibleForTesting
    void printResponseInfo(Response response) {
        if (OK.getStatusCode() == response.getStatus()) {
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
