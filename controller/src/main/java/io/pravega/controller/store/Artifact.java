package io.pravega.controller.store;

import java.util.concurrent.CompletableFuture;

public interface Artifact {

    String getScope();

    /**
     * Get name of stream.
     *
     * @return Name of stream.
     */
    String getName();
    /**
     * Get Scope Name.
     *
     * @return Name of scope.
     */
    String getScopeName();

    /**
     * This method attempts to create a new Waiting Request node and set the processor's name in the node.
     * If a node already exists, this attempt is ignored.
     *
     * @param processorName name of the request processor that is waiting to get an opportunity for processing.
     * @return CompletableFuture which indicates that a node was either created successfully or records the failure.
     */
    CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName);

    /**
     * This method fetches existing waiting request processor's name if any. It returns null if no processor is waiting.
     *
     * @return CompletableFuture which has the name of the processor that had requested for a wait, or null if there was no
     * such request.
     */
    CompletableFuture<String> getWaitingRequestProcessor();

    /**
     * Delete existing waiting request processor if the name of the existing matches suppied processor name.
     *
     * @param processorName processor whose record is to be deleted.
     * @return CompletableFuture which indicates completion of processing.
     */
    CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName);
}
