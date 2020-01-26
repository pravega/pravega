/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.rest.v1;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.nio.Buffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Controller APIs exposed via REST.
 * Different interfaces will hold different groups of APIs.
 *
 * ##############################IMPORTANT NOTE###################################
 * Do not make any API changes here directly, you need to update swagger/Controller.yaml and generate
 * the server stubs as documented in swagger/README.md before updating this file.
 *
 */
public final class ApiV1 {

    @Path("/ping")
    public interface Ping {
        @GET
        Response ping();
    }


    /**
     * Stream metadata version 1.0 APIs.
     */
    @Path("/v1/streams")
    @io.swagger.annotations.Api(description = "the streams API")
    public interface StreamsApi {

        @PUT
        @Path("/stream/segment/{streamSegmentName}/")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Creates a new event", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void append(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "attributeIds", required = true) Long offset,
                @ApiParam(value = "Stream name", required = true) BufferView data,
                @ApiParam(value = "message", required = true) AttributeUpdate[] attributeUpdates,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @PUT
        @Path("/stream/segment/{streamSegmentName}/attributes")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Creates a new event", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void updateAttributes(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "attributeUpdates", required = true) AttributeUpdate[] attributeUpdates,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @PUT
        @Path("/stream/segment/{streamSegmentName}/attributesMap")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Creates a new event", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void getAttributes(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "attributeIds", required = true) UUID[] attributeIds,
                @ApiParam(value = "cache", required = true) Boolean cache,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);


        @GET
        @Path("/stream/segment/{streamSegmentName}/read")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Creates a new event", response = ReadResult.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = ReadResult.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = ReadResult.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = ReadResult.class)})
        public void read(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "attributeIds", required = true) Long offset,
                @ApiParam(value = "cache", required = true) Integer maxLength,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @GET
        @Path("/stream/segment/{streamSegmentName}/info")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Creates a new event", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void getStreamSegmentInfo(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);


        @POST
        @Path("/stream/segment/{streamSegmentName}")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Creates a new segment", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void createStreamSegment(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "attributeUpdates", required = true) AttributeUpdate[] attributeUpdates,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);


        @PUT
        @Path("/stream/segment/{targetStreamSegmentName}/merge")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Merges an existing segment", response = MergeStreamSegmentResult.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = MergeStreamSegmentResult.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = MergeStreamSegmentResult.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = MergeStreamSegmentResult.class)})
        public void mergeStreamSegment(
                @ApiParam(value = "Target Stream Segment name", required = true) @PathParam("targetStreamSegmentName") String targetStreamSegmentName,
                @ApiParam(value = "Source Stream Segment name", required = true)  String sourceStreamSegmentName,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @PUT
        @Path("/stream/segment/{streamSegmentName}/seal")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Merges an existing segment", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void sealStreamSegment(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);


        @DELETE
        @Path("/stream/segment/{streamSegmentName}/")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Merges an existing segment", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void deleteStreamSegment(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        @PUT
        @Path("/stream/segment/{streamSegmentName}/truncate")
        @Consumes("text/plain")
        @Produces({"application/json"})
        @ApiOperation(
                value = "", notes = "Merges an existing segment", response = Long.class, tags = {})
        @ApiResponses(value = {
                @ApiResponse(
                        code = 201, message = "Successfully created the event", response = Long.class),

                @ApiResponse(
                        code = 409, message = "Event already exists", response = Long.class),

                @ApiResponse(
                        code = 500, message = "Server error", response = Long.class)})
        public void truncateStreamSegment(
                @ApiParam(value = "Stream Segment name", required = true) @PathParam("streamSegmentName") String streamSegmentName,
                @ApiParam(value = "offset", required = true) Long offset,
                @ApiParam(value = "duration", required = true) Duration duration,
                @Context SecurityContext securityContext, @Suspended final AsyncResponse asyncResponse);

        //endregion


    }
}
