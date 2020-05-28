/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store;

import io.pravega.common.concurrent.Futures;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public interface ArtifactStore {

    Artifact getArtifact(String scope, String stream, OperationContext context);

    /**
     * Method to create an operation context. A context ensures that multiple calls to store for the same data are avoided
     * within the same operation. All api signatures are changed to accept context. If context is supplied, the data will be
     * looked up within the context and, upon a cache miss, will be fetched from the external store and cached within the context.
     * Once an operation completes, the context is discarded.
     *
     * @param scope Stream scope.
     * @param name  Stream name.
     * @return Return a streamContext
     */
    OperationContext createContext(final String scope, final String name);

    /**
     * This method fetches existing waiting request processor's name if any. It returns null if no processor is waiting.
     *
     * @param scope scope name
     * @param name stream/kvt name
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which has the name of the processor that had requested for a wait, or null if there was no
     * such request.
     */
    default CompletableFuture<String> getWaitingRequestProcessor(String scope, String name, OperationContext context, ScheduledExecutorService executor) {
        return Futures.withCompletion(getArtifact(scope, name, context).getWaitingRequestProcessor(), executor);
    }

    /**
     * This method attempts to create a new Waiting Request node and set the processor's name in the node.
     * If a node already exists, this attempt is ignored.
     *
     * @param scope scope
     * @param stream stream
     * @param processorName name of the request processor that is waiting to get an opportunity for processing.
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which indicates that a node was either created successfully or records the failure.
     */
    default CompletableFuture<Void> createWaitingRequestIfAbsent(String scope, String stream, String processorName,
                                                                OperationContext context, ScheduledExecutorService executor) {
        return Futures.withCompletion(getArtifact(scope, stream, context).createWaitingRequestIfAbsent(processorName), executor);
    }

    /**
     * Delete existing waiting request processor if the name of the existing matches suppied processor name.
     *
     * @param scope scope
     * @param stream stream
     * @param processorName processor name which is to be deleted if it matches the name in waiting record in the store.
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which indicates completion of processing.
     */
    default CompletableFuture<Void> deleteWaitingRequestConditionally(String scope, String stream, String processorName,
                                                                     OperationContext context, ScheduledExecutorService executor) {
        return Futures.withCompletion(getArtifact(scope, stream, context).deleteWaitingRequestConditionally(processorName), executor);
    }

}
