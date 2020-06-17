/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.rest.resources;

import io.pravega.auth.AuthException;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.server.rpc.auth.AuthHandlerManager;
import io.pravega.segmentstore.server.rpc.auth.RESTAuthHelper;
import io.pravega.segmentstore.server.store.StreamSegmentService;
import io.pravega.segmentstore.server.host.rest.v1.ApiV1;
import io.pravega.shared.NameUtils;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import lombok.extern.slf4j.Slf4j;
import static io.pravega.auth.AuthHandler.Permissions;
import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;

import java.time.Duration;
import java.util.UUID;



/**
 * Stream metadata resource implementation.
 */
@Slf4j
public class StreamDataResourceImpl implements ApiV1.StreamsApi {

    @Context
    HttpHeaders headers;

    private final StreamSegmentService streamSegmentService;
    private final RESTAuthHelper restAuthHelper;
    private final ConnectionFactory connectionFactory;

    public StreamDataResourceImpl(StreamSegmentService streamSegmentService, AuthHandlerManager pravegaAuthManager, ConnectionFactory connectionFactory) {
        this.streamSegmentService = streamSegmentService;
        this.restAuthHelper = new RESTAuthHelper(pravegaAuthManager);
        this.connectionFactory = connectionFactory;
    }

    private boolean validateUserStreamSegmentName(String streamSegmentName, String methodName) {
        try {
            NameUtils.validateUserStreamSegmentName(streamSegmentName);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Stream segment name {} is invalid for invoking append",
                    streamSegmentName);
            return false;
        }
        log.info("{} called for streamSegmentName:{}", methodName, streamSegmentName);
        return true;
    }

    private int checkAuthorization(String streamSegmentName, String methodName, Permissions permission) {
        try {
            restAuthHelper.authenticateAuthorize(
                    getAuthorizationHeader(),
                    "event", permission);
        } catch (AuthException e) {
            log.warn("{} failed due to authentication failure.", methodName);
            return e.getResponseCode();
        }
        log.info("{} authorized", methodName);
        return 0;
    }

    private List<String> getAuthorizationHeader() {
        return headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
    }

    private List<AttributeUpdate> toList(AttributeUpdate[] attributeUpdates) {
        List<AttributeUpdate> attributeUpdatesList = new ArrayList<>();
        for (AttributeUpdate attributeUpdate : attributeUpdates) {
            attributeUpdatesList.add(attributeUpdate);
        }
        return attributeUpdatesList;
    }

    private List<UUID> toList(UUID[] uuids) {
        List<UUID> uuidList = new ArrayList<>();
        for (UUID uuid : uuids) {
           uuidList.add(uuid);
        }
        return uuidList;
    }

    /**
     * Appends a range of bytes at the end of a StreamSegment an atomically updates the given
     * attributes, but only if the current length of the StreamSegment equals a certain value. The
     * byte range will be appended as a contiguous block. This method guarantees ordering (among
     * subsequent calls).
     *
     * @param streamSegmentName The name of the StreamSegment to append to.
     * @param offset            The offset at which to append. If the current length of the StreamSegment does not equal
     *                          this value, the operation will fail with a BadOffsetException.
     * @param data              A {@link BufferView} representing the data to add. This {@link BufferView} should not be
     *                          modified until the returned @Override
public void from this method completes.
     * @param attributeUpdates  A Collection of Attribute-Values to set or update. May be null (which indicates no updates).
     *                          See Notes about AttributeUpdates in the interface Javadoc.
     * @param timeout           Timeout for the operation
     * @throws NullPointerException If any of the arguments are null, except attributeUpdates.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't
     *                                  check if the StreamSegment does not exist - that exception
     *                                  will be set in the returned @Override
public void).
     */
    @Override
    public void append(String streamSegmentName, Long offset, BufferView data, AttributeUpdate[] attributeUpdates, Duration timeout, final SecurityContext securityContext,
                       final AsyncResponse asyncResponse) {
        String methodName = "append";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ_UPDATE);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.append(streamSegmentName, offset, data, toList(attributeUpdates), timeout)
                .thenApply(response -> {
                    return Response.status(Status.OK).entity(response).build();
                })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Performs an attribute update operation on the given Segment.
     *
     * @param streamSegmentName The name of the StreamSegment which will have its attributes updated.
     * @param attributeUpdates  A Collection of Attribute-Values to set or update. May be null (which indicates no updates).
     *                          See Notes about AttributeUpdates in the interface Javadoc.
     * @param timeout           Timeout for the operation
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't check if the StreamSegment
     *                                  does not exist - that exception will be set in the returned.
     */
    @Override
    public void updateAttributes(String streamSegmentName, AttributeUpdate[] attributeUpdates, Duration timeout, final SecurityContext securityContext,
                                 final AsyncResponse asyncResponse) {
        String methodName = "updateAttributes";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ_UPDATE);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.updateAttributes(streamSegmentName, toList(attributeUpdates), timeout)
                .thenApply(response -> {
                return Response.status(Status.OK).entity(response).build();
            })
            .exceptionally( exception -> {
                    log.warn(" failed with exception: {}", methodName, exception);
                    return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
            }).thenApply(asyncResponse::resume)
            .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Gets the values of the given Attributes (Core or Extended).
     *
     * Lookup order:
     * 1. (Core or Extended) In-memory Segment Metadata cache (which always has the latest value of an attribute).
     * 2. (Extended only) Backing Attribute Index for this Segment.
     *
     * @param streamSegmentName The name of the StreamSegment for which to get attributes.
     * @param attributeIds      A Collection of Attribute Ids to fetch. These may be Core or Extended Attributes.
     * @param cache             If set, then any Extended Attribute values that are not already in the in-memory Segment
     *                          Metadata cache will be atomically added using a conditional update (comparing against a missing value).
     *                          This argument will be ignored if the StreamSegment is currently Sealed.
     * @param timeout           Timeout for the operation.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't check if the StreamSegment
     *                                  does not exist - that exception will be set in the returned.
     */
    @Override
    public void getAttributes(String streamSegmentName, UUID[] attributeIds, Boolean cache, Duration timeout, final SecurityContext securityContext,
                                               final AsyncResponse asyncResponse) {
        String methodName = "getAttributes";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.getAttributes(streamSegmentName, toList(attributeIds), cache, timeout)
                .thenApply(response -> {
                return Response.status(Status.OK).entity(response).build();
            })
            .exceptionally( exception -> {
                    log.warn(" failed with exception: {}", methodName, exception);
                    return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
            }).thenApply(asyncResponse::resume)
            .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Initiates a Read operation on a particular StreamSegment and returns a ReadResult which can be used to consume the
     * read data.
     *
     * @param streamSegmentName The name of the StreamSegment to read from.
     * @param offset            The offset within the stream to start reading at.
     * @param maxLength         The maximum number of bytes to read.
     * @param timeout           Timeout for the operation.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    @Override
    public void read(String streamSegmentName, Long offset, Integer maxLength, Duration timeout, final SecurityContext securityContext,
                     final AsyncResponse asyncResponse) {
        String methodName = "read";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.read(streamSegmentName, offset, maxLength, timeout)
                .thenApply(response -> {
            return Response.status(Status.OK).entity(response).build();
        })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Gets information about a StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    @Override
    public void getStreamSegmentInfo(String streamSegmentName, Duration timeout, final SecurityContext securityContext,
                                     final AsyncResponse asyncResponse) {
        String methodName = "getStreamSegmentInfo";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.getStreamSegmentInfo(streamSegmentName, timeout)
                .thenApply(response -> {
                    return Response.status(Status.OK).entity(response).build();
                })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Creates a new StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment to create.
     * @param attributes        A Collection of Attribute-Values to set on the newly created StreamSegment. May be null.
     *                          See Notes about AttributeUpdates in the interface Javadoc.
     * @param timeout           Timeout for the operation.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    @Override
    public void createStreamSegment(String streamSegmentName, AttributeUpdate[] attributes, Duration timeout, final SecurityContext securityContext,
                                    final AsyncResponse asyncResponse) {
        String methodName = "createStreamSegment";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ_UPDATE);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.createStreamSegment(streamSegmentName, toList(attributes), timeout)
                .thenApply(response -> {
                    return Response.status(Status.OK).entity(response).build();
                })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));

    }

    /**
     * Merges a StreamSegment into another. If the StreamSegment is not already sealed, it will seal it.
     *
     * @param targetSegmentName The name of the StreamSegment to merge into.
     * @param sourceSegmentName The name of the StreamSegment to merge.
     * @param timeout           Timeout for the operation.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    @Override
    public void mergeStreamSegment(String targetSegmentName, String sourceSegmentName, Duration timeout, final SecurityContext securityContext,
                                   final AsyncResponse asyncResponse) {
        String methodName = "mergeStreamSegment";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(targetSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(targetSegmentName, methodName, READ_UPDATE);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.mergeStreamSegment(targetSegmentName, sourceSegmentName, timeout)
                .thenApply(response -> {
                    return Response.status(Status.OK).entity(response).build();
                })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Seals a StreamSegment for modifications.
     *
     * @param streamSegmentName The name of the StreamSegment to seal.
     * @param timeout           Timeout for the operation
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    @Override
    public void sealStreamSegment(String streamSegmentName, Duration timeout, final SecurityContext securityContext,
                                  final AsyncResponse asyncResponse) {
        String methodName = "sealStreamSegment";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ_UPDATE);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.sealStreamSegment(streamSegmentName, timeout)
                .thenApply(response -> {
                    return Response.status(Status.OK).entity(response).build();
                })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Deletes a StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment to delete.
     * @param timeout           Timeout for the operation.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    @Override
    public void deleteStreamSegment(String streamSegmentName, Duration timeout, final SecurityContext securityContext,
                                    final AsyncResponse asyncResponse) {
        String methodName = "deleteStreamSegment";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ_UPDATE);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.deleteStreamSegment(streamSegmentName, timeout)
                .thenApply(response -> {
                    return Response.status(Status.OK).entity(response).build();
                })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));
    }

    /**
     * Truncates a StreamSegment at a given offset.
     *
     * @param streamSegmentName The name of the StreamSegment to truncate.
     * @param offset            The offset at which to truncate. This must be at least equal to the existing truncation
     *                          offset and no larger than the StreamSegment's length. After the operation is complete,
     *                          no offsets below this one will be accessible anymore.
     * @param timeout           Timeout for the operation.
     */

    @Override
    public void truncateStreamSegment(String streamSegmentName, Long offset, Duration timeout, final SecurityContext securityContext,
                                      final AsyncResponse asyncResponse) {
        String methodName = "truncateStreamSegment";
        long traceId = LoggerHelpers.traceEnter(log, methodName);
        if (!validateUserStreamSegmentName(streamSegmentName, methodName)) {
            asyncResponse.resume(Response.status(Status.BAD_REQUEST).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        int responseCode = checkAuthorization(streamSegmentName, methodName, READ_UPDATE);
        if (responseCode != 0) {
            asyncResponse.resume(Response.status(Status.fromStatusCode(responseCode)).build());
            LoggerHelpers.traceLeave(log, methodName, traceId);
            return;
        }
        streamSegmentService.truncateStreamSegment(streamSegmentName, offset, timeout)
                .thenApply(response -> {
                    return Response.status(Status.OK).entity(response).build();
                })
                .exceptionally( exception -> {
                        log.warn(" failed with exception: {}", methodName, exception);
                        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
                }).thenApply(asyncResponse::resume)
                .thenAccept(x -> LoggerHelpers.traceLeave(log, methodName, traceId));

    }

}
