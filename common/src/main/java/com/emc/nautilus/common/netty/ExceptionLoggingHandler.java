package com.emc.nautilus.common.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExceptionLoggingHandler extends ChannelDuplexHandler {

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
            Throwable cause) throws Exception {
    	log.error("Uncaught exception on connection: ", cause);
        super.exceptionCaught(ctx, cause);
    }
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			super.channelRead(ctx, msg);
		} catch (Exception e) {
			log.error("Uncaught exception on connection: ", e);
			throw e;
		}
	}

    
}
