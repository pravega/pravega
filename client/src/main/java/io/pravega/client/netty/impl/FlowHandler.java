/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.ScheduledFuture;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ReusableFutureLatch;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlowHandler extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private static final int FLOW_DISABLED = 0;
    private final String connectionName;
    private final AtomicReference<Channel> channel = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> keepAliveFuture = new AtomicReference<>();
    private final AtomicBoolean recentMessage = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @Getter
    private final ReusableFutureLatch<Void> registeredFutureLatch = new ReusableFutureLatch<>();
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<Integer, ReplyProcessor> flowIdReplyProcessorMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AppendBatchSizeTracker> flowIDBatchSizeTrackerMap = new ConcurrentHashMap<>();

    private final AtomicBoolean disableFlow = new AtomicBoolean(false);

    public FlowHandler(String connectionName) {
        this.connectionName = connectionName;
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
        log.info("Creating Flow {} for endpoint {}. The current Channel is {}.", flow.getFlowId(), connectionName, channel.get());
        if (flowIdReplyProcessorMap.put(flowID, rp) != null) {
            throw new IllegalArgumentException("Multiple flows cannot be created with the same Flow id " + flowID);
        }
        createAppendBatchSizeTrackerIfNeeded(flowID);
        return new ClientConnectionImpl(connectionName, flowID, this);
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
        log.info("Creating a new connection with flow disabled for endpoint {}. The current Channel is {}.", connectionName, channel.get());
        flowIdReplyProcessorMap.put(FLOW_DISABLED, rp);
        createAppendBatchSizeTrackerIfNeeded(FLOW_DISABLED);
        return new ClientConnectionImpl(connectionName, FLOW_DISABLED, this);
    }

    /**
     * Close a flow. This is invoked when the ClientConnection is closed.
     * @param clientConnection Client Connection.
     */
    public void closeFlow(ClientConnection clientConnection) {
        final ClientConnectionImpl clientConnectionImpl = (ClientConnectionImpl) clientConnection;
        int flow = clientConnectionImpl.getFlowId();
        log.info("Closing Flow {} for endpoint {}", flow, clientConnectionImpl.getConnectionName());
        flowIdReplyProcessorMap.remove(flow);
        flowIDBatchSizeTrackerMap.remove(flow);
        if (flow == FLOW_DISABLED) {
            // close the channel immediately since this netty channel will not be reused by other flows.
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
        return flowIDBatchSizeTrackerMap.get(Flow.toFlowID(requestID));
    }

    /**
     * Returns the number of open flows.
     * @return Flow count.
     */
    public int getOpenFlowCount() {
        return flowIdReplyProcessorMap.size();
    }
    
    /**
     * Check the current status of Connection.
     * @return True if the connection is established.
     */
    public boolean isConnectionEstablished() {
        return channel.get() != null;
    }

    /**
     * Fetch the netty channel. If {@link Channel} is null then throw a ConnectionFailedException.
     * @return  The current {@link Channel}
     * @throws ConnectionFailedException Throw if connection is not established.
     */
    Channel getChannel() throws ConnectionFailedException {
        Channel ch = channel.get();
        if (ch == null) {
            throw new ConnectionFailedException("Connection to " + connectionName + " is not established.");
        }
        return ch;
    }

    /**
     * Set the Recent Message flag. This is used to avoid sending redundant KeepAlives over the connection.
     */
    void setRecentMessage() {
        recentMessage.set(true);
    }

    /**
     * This function completes the input future when the channel is registered.
     *
     * @param future CompletableFuture which will be completed once the channel is registered.
     */
    void completeWhenRegistered(final CompletableFuture<Void> future) {
        Preconditions.checkNotNull(future, "future");
        registeredFutureLatch.register(future);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        Channel ch = ctx.channel();
        channel.set(ch);
        log.info("Connection established with endpoint {} on ChannelId {}", connectionName, ch);
        ch.write(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION), ch.voidPromise());
        registeredFutureLatch.release(null); //release all futures waiting for channel registration to complete.
        // WireCommands.KeepAlive messages are sent for every network connection to a SegmentStore.
        ScheduledFuture<?> old = keepAliveFuture.getAndSet(ch.eventLoop().scheduleWithFixedDelay(new KeepAliveTask(), 20, 10, TimeUnit.SECONDS));
        if (old != null) {
            old.cancel(false);
        }
    }

    /**
     * Invoke all the {@link ReplyProcessor#connectionDropped()} for all the registered flows once the
     * connection is disconnected.
     *
     * @see io.netty.channel.ChannelInboundHandler#channelUnregistered(ChannelHandlerContext)
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        registeredFutureLatch.reset();
        ScheduledFuture<?> future = keepAliveFuture.get();
        if (future != null) {
            future.cancel(false);
        }
        channel.set(null);
        flowIdReplyProcessorMap.forEach((flowId, rp) -> {
            try {
                log.debug("Connection dropped for flow id {}", flowId);
                rp.connectionDropped();
            } catch (Exception e) {
                // Suppressing exception which prevents all ReplyProcessor.connectionDropped from being invoked.
                log.warn("Encountered exception invoking ReplyProcessor for flow id {}", flowId, e);
            }
        });
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Reply cmd = (Reply) msg;
        log.debug(connectionName + " processing reply {} with flow {}", cmd, Flow.from(cmd.getRequestId()));

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
                batchSizeTracker.recordAck(dataAppended.getEventNumber());
            }
        }
        // Obtain ReplyProcessor and process the reply.
        getReplyProcessor(cmd).ifPresent(processor -> {
            try {
                processor.process(cmd);
            } catch (Exception e) {
                log.warn("ReplyProcessor.process failed for reply {} due to {}", cmd, e.getMessage());
                processor.processingFailure(e);
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
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
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Channel ch = channel.get();
            if (ch != null) {
                log.debug("Closing channel {} ", ch);
                final int openFlowCount = flowIdReplyProcessorMap.size();
                if (openFlowCount != 0) {
                    log.warn("{} flows are not closed", openFlowCount);
                }
                final int appendTrackerCount = flowIDBatchSizeTrackerMap.size();
                if (appendTrackerCount != 0) {
                    log.warn("{} AppendBatchSizeTrackers are not closed", appendTrackerCount);
                }
                ch.close();
            }
        }
    }

    private final class KeepAliveTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!recentMessage.getAndSet(false)) {
                    Futures.getAndHandleExceptions(getChannel().writeAndFlush(new WireCommands.KeepAlive()), ConnectionFailedException::new);
                }
            } catch (Exception e) {
                log.warn("Keep alive failed, killing connection {} due to {}", connectionName, e.getMessage());
                close();
            }
        }
    }

    private Optional<ReplyProcessor> getReplyProcessor(Reply cmd) {
        int flowId = disableFlow.get() ? FLOW_DISABLED : Flow.toFlowID(cmd.getRequestId());
        final ReplyProcessor processor = flowIdReplyProcessorMap.get(flowId);
        if (processor == null) {
            log.warn("No ReplyProcessor found for the provided flowId {}. Ignoring response", flowId);
        }
        return Optional.ofNullable(processor);
    }
}
