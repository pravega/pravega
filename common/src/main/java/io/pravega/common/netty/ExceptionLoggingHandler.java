/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Used to make sure any stray exceptions that make it back to the socket get logged.
 */
@Slf4j
public class ExceptionLoggingHandler extends ChannelDuplexHandler {

    private final String connectionName;

    public ExceptionLoggingHandler(String connectionName) {
        this.connectionName = connectionName;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Uncaught exception on connection " + connectionName, cause);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            super.channelRead(ctx, msg);
        } catch (Exception e) {
            log.error("Uncaught exception on connection " + connectionName, e);
            throw e;
        }
    }

}
