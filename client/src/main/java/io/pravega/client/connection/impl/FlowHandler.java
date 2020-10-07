/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.NameUtils.writerTags;
import static io.pravega.shared.metrics.ClientMetricKeys.CLIENT_OUTSTANDING_APPEND_COUNT;

@Slf4j
public class FlowHandler extends FailingReplyProcessor implements AutoCloseable {

    private static final int FLOW_DISABLED = 0;
    private final PravegaNodeUri location;
    private ClientConnection channel; //Final (set in factory after construction)
    @Getter
    private final MetricNotifier metricNotifier;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final KeepAliveTask keepAliveTask = new KeepAliveTask();
    private ScheduledFuture<?> keepAliveFuture; //Final (set in factory after construction)
    private final AtomicBoolean recentMessage = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<Integer, ReplyProcessor> flowIdReplyProcessorMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AppendBatchSizeTracker> flowIDBatchSizeTrackerMap = new ConcurrentHashMap<>();
    private final AtomicBoolean disableFlow = new AtomicBoolean(false);

    private FlowHandler(PravegaNodeUri location, MetricNotifier updateMetric) {
        this.location = location;
        this.metricNotifier = updateMetric;
    }

    static CompletableFuture<FlowHandler> openConnection(PravegaNodeUri location, MetricNotifier updateMetric, ConnectionFactory connectionFactory) {
        FlowHandler flowHandler = new FlowHandler(location, updateMetric);
        return connectionFactory.establishConnection(location, flowHandler).thenApply(connection -> {
            flowHandler.channel = connection;
            flowHandler.keepAliveFuture = connectionFactory.getInternalExecutor().scheduleAtFixedRate(flowHandler.keepAliveTask, 20, 10, TimeUnit.SECONDS);
            try {
                connection.send(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
            } catch (ConnectionFailedException e) {
                throw Exceptions.sneakyThrow(e);
            }
            return flowHandler;
        });
    }
    
    /**
     * Create a flow on existing connection.
     * @param flow Flow.
     * @param rp ReplyProcessor for the specified flow.
     * @return Client Connection object.
     */
    public ClientConnection createFlow(final Flow flow, final ReplyProcessor rp) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkState(!disableFlow.get(), "Ensure flows are enabled.");
        final int flowID = flow.getFlowId();
        log.info("Creating Flow {} for endpoint {}. ", flow.getFlowId(), location);
        if (flowIdReplyProcessorMap.put(flowID, rp) != null) {
            throw new IllegalArgumentException("Multiple flows cannot be created with the same Flow id " + flowID);
        }
        createAppendBatchSizeTrackerIfNeeded(flowID);
        return new FlowClientConnection(location.toString(), channel, flowID, this);
    }

    /**
     * Create a {@link ClientConnection} where flows are disabled. This implies that there is only one flow on the underlying
     * network connection.
     * @param rp  The ReplyProcessor.
     * @return Client Connection object.
     */
    public ClientConnection createConnectionWithFlowDisabled(final ReplyProcessor rp) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkState(!disableFlow.getAndSet(true), "Flows are disabled, incorrect usage pattern.");
        log.info("Creating a new connection with flow disabled for endpoint {}.", location);
        flowIdReplyProcessorMap.put(FLOW_DISABLED, rp);
        createAppendBatchSizeTrackerIfNeeded(FLOW_DISABLED);
        return new FlowClientConnection(location.toString(), channel, FLOW_DISABLED, this);
    }

    /**
     * Close a flow. This is invoked when the ClientConnection is closed.
     * @param clientConnection Client Connection.
     */
    void closeFlow(FlowClientConnection clientConnection) {
        int flow = clientConnection.getFlowId();
        log.info("Closing Flow {} for endpoint {}", flow, clientConnection.getConnectionName());
        flowIdReplyProcessorMap.remove(flow);
        flowIDBatchSizeTrackerMap.remove(flow);
        if (flow == FLOW_DISABLED) {
            // close the channel immediately since this connection will not be reused by other flows.
            close();
        }
    }

    /**
     * Create a Batch size tracker, ignore if already existing
     *
     * @param flowID flow ID.
     */
    private void createAppendBatchSizeTrackerIfNeeded(final int  flowID) {
        if (flowIDBatchSizeTrackerMap.containsKey(flowID)) {
            log.debug("Reusing Batch size tracker for Flow ID {}.", flowID);
        } else {
            log.debug("Creating Batch size tracker for flow ID {}.", flowID);
            flowIDBatchSizeTrackerMap.put(flowID, new AppendBatchSizeTrackerImpl());
        }
    }

    /**
     * Get a Batch size tracker for requestID.
     *
     * @param requestID flow ID.
     * @return Batch size Tracker object.
     */
    public AppendBatchSizeTracker getAppendBatchSizeTracker(final long requestID) {
        int flowID;
        if (disableFlow.get()) {
            flowID = FLOW_DISABLED;
        } else {
            flowID = Flow.toFlowID(requestID);
        }
        return flowIDBatchSizeTrackerMap.get(flowID);
    }

    /**
     * Returns the number of open flows.
     * @return Flow count.
     */
    public int getOpenFlowCount() {
        return flowIdReplyProcessorMap.size();
    }

    /**
     * Set the Recent Message flag. This is used to avoid sending redundant KeepAlives over the connection.
     */
    void setRecentMessage() {
        recentMessage.set(true);
    }

    @Override
    public void process(Reply cmd) {
        if (log.isDebugEnabled()) {
            log.debug("{} processing reply {} with flow {}", location, cmd, Flow.from(cmd.getRequestId()));
        }
        setRecentMessage();
        if (cmd instanceof WireCommands.Hello) {
            flowIdReplyProcessorMap.forEach((flowId, rp) -> {
                try {
                    rp.hello((WireCommands.Hello) cmd);
                } catch (Exception e) {
                    // Suppressing exception which prevents all ReplyProcessor.hello from being invoked.
                    log.warn("Encountered exception invoking ReplyProcessor.hello for flow id {}", flowId, e);
                }
            });
            return;
        }

        if (cmd instanceof WireCommands.DataAppended) {
            final WireCommands.DataAppended dataAppended = (WireCommands.DataAppended) cmd;
            final AppendBatchSizeTracker batchSizeTracker = getAppendBatchSizeTracker(dataAppended.getRequestId());
            if (batchSizeTracker != null) {
                long pendingAckCount = batchSizeTracker.recordAck(dataAppended.getEventNumber());
                // Only publish client side metrics when there is some metrics notifier configured for efficiency.
                if (!metricNotifier.equals(MetricNotifier.NO_OP_METRIC_NOTIFIER)) {
                    metricNotifier.updateSuccessMetric(CLIENT_OUTSTANDING_APPEND_COUNT, writerTags(dataAppended.getWriterId().toString()),
                            pendingAckCount);
                }
            }
        }
        // Obtain ReplyProcessor and process the reply.
        ReplyProcessor processor = getReplyProcessor(cmd);
        if (processor != null) {
            try {
                processor.process(cmd);
            } catch (Exception e) {
                log.warn("ReplyProcessor.process failed for reply {} due to {}", cmd, e.getMessage());
                processor.processingFailure(e);
            }
        } else {
            if (cmd instanceof WireCommands.ReleasableCommand) {
                ((WireCommands.ReleasableCommand) cmd).release();
            }
        }
    }

    @Override
    public void processingFailure(Exception error) {
        invokeProcessingFailureForAllFlows(error);
    }

    private void invokeProcessingFailureForAllFlows(Throwable cause) {
        flowIdReplyProcessorMap.forEach((flowId, rp) -> {
            try {
                log.debug("Exception observed for flow id {} due to {}", flowId, cause.getMessage());
                rp.processingFailure(new ConnectionFailedException(cause));
            } catch (Exception e) {
                // Suppressing exception which prevents all ReplyProcessor.processingFailure from being invoked.
                log.warn("Encountered exception invoking ReplyProcessor.processingFailure for flow id {}", flowId, e);
            }
        });
    }

    @Override
    public void connectionDropped() {
        close();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (keepAliveFuture != null) {
                keepAliveFuture.cancel(false);
            }
            log.info("Connection closed observed with endpoint {}", location);
            flowIdReplyProcessorMap.forEach((flowId, rp) -> {
                try {
                    log.debug("Connection dropped for flow id {}", flowId);
                    rp.connectionDropped();
                } catch (Exception e) {
                    // Suppressing exception which prevents all ReplyProcessor.connectionDropped
                    // from being invoked.
                    log.warn("Encountered exception invoking ReplyProcessor for flow id {}", flowId, e);
                }
            });
            final int appendTrackerCount = flowIDBatchSizeTrackerMap.size();
            if (appendTrackerCount != 0) {
                log.warn("{} AppendBatchSizeTrackers are not closed", appendTrackerCount);
            }
            channel.close();
        }
    }
    
    public final boolean isClosed() {
        return closed.get();
    }

    @VisibleForTesting
    final class KeepAliveTask implements Runnable {
        private final AtomicInteger concurrentlyRunning = new AtomicInteger(0);
        @Override
        public void run() {
            try {
                if (!recentMessage.getAndSet(false) && !closed.get()) {
                    int running = concurrentlyRunning.getAndIncrement();
                    if (running > 0) {
                        handleError(new TimeoutException("KeepAliveTask: Connection write was blocked for too long."));
                    }
                    channel.send(new WireCommands.KeepAlive());
                    concurrentlyRunning.decrementAndGet();
                }
            } catch (Exception e) {
                handleError(e);
            }
        }
        
        @VisibleForTesting
        void handleError(Exception e) {
            log.warn("Failed to send KeepAlive to {}. Closing this connection.", location, e);
            close();
        }
    }

    private ReplyProcessor getReplyProcessor(Reply cmd) {
        int flowId = disableFlow.get() ? FLOW_DISABLED : Flow.toFlowID(cmd.getRequestId());
        final ReplyProcessor processor = flowIdReplyProcessorMap.get(flowId);
        if (processor == null) {
            log.warn("No ReplyProcessor found for the provided flowId {}. Ignoring response", flowId);
            if (cmd instanceof WireCommands.ReleasableCommand) {
                ((WireCommands.ReleasableCommand) cmd).release();
            }
        }
        return processor;
    }

}
