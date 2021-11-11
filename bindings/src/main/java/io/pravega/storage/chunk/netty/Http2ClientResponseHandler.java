package io.pravega.storage.chunk.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Http2ClientResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

    private final Logger logger = LoggerFactory.getLogger(Http2ClientResponseHandler.class);
    private final Map<String, ResponseFuture> streamidMap;

    public Http2ClientResponseHandler() {
        streamidMap = new ConcurrentHashMap<>();
    }

    public ResponseFuture put(String streamId, ChannelFuture writeFuture, ChannelPromise promise) {
        return streamidMap.put(streamId, new ResponseFuture(writeFuture, promise));
    }

    public void awaitResponses(long timeout, TimeUnit unit) {

        Iterator<Entry<String, ResponseFuture>> itr = streamidMap.entrySet()
            .iterator();

        while (itr.hasNext()) {
            Entry<String, ResponseFuture> entry = itr.next();
            ChannelFuture writeFuture = entry.getValue()
                .getWriteFuture();

            if (!writeFuture.awaitUninterruptibly(timeout, unit)) {
                throw new IllegalStateException("Timed out waiting to write for stream id " + entry.getKey());
            }
            if (!writeFuture.isSuccess()) {
                throw new RuntimeException(writeFuture.cause());
            }
            ChannelPromise promise = entry.getValue()
                .getPromise();

            if (!promise.awaitUninterruptibly(timeout, unit)) {
                throw new IllegalStateException("Timed out waiting for response on stream id " + entry.getKey());
            }
            if (!promise.isSuccess()) {
                throw new RuntimeException(promise.cause());
            }
            logger.info("---Stream id: " + entry.getKey() + " received---");
            
            itr.remove();
        }

    }

    public HttpResponse awaitResponse(String streamId, long timeout, TimeUnit unit) {
        ResponseFuture future = streamidMap.get(streamId);

        ChannelFuture writeFuture = future
                .getWriteFuture();

        if (!writeFuture.awaitUninterruptibly(timeout, unit)) {
            throw new IllegalStateException("Timed out waiting to write for stream id " + streamId);
        }
        if (!writeFuture.isSuccess()) {
            throw new RuntimeException(writeFuture.cause());
        }
        ChannelPromise promise = future
                .getPromise();

        if (!promise.awaitUninterruptibly(timeout, unit)) {
            throw new IllegalStateException("Timed out waiting for response on stream id " + streamId);
        }
        if (!promise.isSuccess()) {
            throw new RuntimeException(promise.cause());
        }
        logger.info("---Stream id: {} received---",streamId);
        streamidMap.remove(streamId);
        return future.getResponse();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        String streamId = msg.headers()
            .get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
        if (streamId == null) {
            logger.error("HttpResponseHandler unexpected message received: " + msg);
            return;
        }

        ResponseFuture value = streamidMap.get(streamId);

        if (value == null) {
            logger.error("Message received for unknown stream id " + streamId);
            ctx.close();
        } else {
            ByteBuf content = msg.content();
            if (content.isReadable()) {
                int contentLength = content.readableBytes();
                byte[] arr = new byte[contentLength];
                content.readBytes(arr);
                String response = new String(arr, 0, contentLength, CharsetUtil.UTF_8);
                logger.info("Response from Server: "+ (response));

            }
//
            value.setResponse(msg);
            value.getPromise()
                .setSuccess();
        }
    }

    public static class ResponseFuture {
        volatile ChannelFuture writeFuture;
        volatile ChannelPromise promise;
        volatile HttpResponse response;


        public HttpResponse getResponse() {
            return response;
        }

        public void setResponse(HttpResponse response) {
            this.response = response;
        }



        ResponseFuture(ChannelFuture writeFuture, ChannelPromise promise) {
            this.writeFuture = writeFuture;
            this.promise = promise;
        }

        ChannelFuture getWriteFuture() {
            return writeFuture;
        }

        ChannelPromise getPromise() {
            return promise;
        }

    }
}
