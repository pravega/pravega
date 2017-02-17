/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest.v1;

import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.PathParam;
import javax.ws.rs.PUT;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Controller APIs exposed via REST.
 * Different interfaces will hold different groups of APIs
 */
public final class ApiV1 {

    @Path("/ping")
    public static interface Ping {
        @GET
        public Response ping();
    }

    /**
     * Stream metadata version 1.0 APIs.
     */
    @Path("/v1")
    public static interface StreamMetadata {

        /**
         * REST API to create a stream.
         *
         * @param scope               The scope of stream
         * @param createStreamRequest The object conforming to createStream request json
         * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing
         */
        @POST
        @Path("/scopes/{scope}/streams")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public void createStream(@PathParam("scope") final String scope, @Valid final CreateStreamRequest createStreamRequest,
                                 @Suspended final AsyncResponse asyncResponse);

        /**
         * REST API to update stream configuration.
         *
         * @param scope               The scope of stream
         * @param stream              The name of stream
         * @param updateStreamRequest The object conforming to updateStreamConfig request json
         * @param asyncResponse       AsyncResponse provides means for asynchronous server side response processing
         */
        @PUT
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        @Consumes(MediaType.APPLICATION_JSON)
        public void updateStreamConfig(@PathParam("scope") final String scope, @PathParam("stream") final String stream,
                                       @Valid final UpdateStreamRequest updateStreamRequest,
                                       @Suspended final AsyncResponse asyncResponse);

        /**
         * REST API to get configuration of a stream.
         *
         * @param scope         The scope of stream
         * @param stream        The name of stream
         * @param asyncResponse AsyncResponse provides means for asynchronous server side response processing
         */
        @GET
        @Path("/scopes/{scope}/streams/{stream}")
        @Produces(MediaType.APPLICATION_JSON)
        public void getStreamConfig(@PathParam("scope") final String scope, @PathParam("stream") final String stream,
                                    @Suspended final AsyncResponse asyncResponse);
    }
}
