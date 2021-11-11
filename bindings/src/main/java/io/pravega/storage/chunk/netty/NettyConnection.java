package io.pravega.storage.chunk.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.pravega.storage.ECSChunkConnection;
import io.pravega.storage.chunk.ECSChunkStorageConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class NettyConnection implements ECSChunkConnection {
    private static final Logger log = LoggerFactory.getLogger(NettyConnection.class);
    private final String name;
    private final InetSocketAddress socketAddress;
    private Channel channel;
    private volatile CompletableFuture<HttpResponse> responseFuture;
    private final ECSChunkStorageConfig config;
    private final URI configUri;
    private Http2SettingsHandler http2SettingsHandler;
    private Http2ClientResponseHandler responseHandler;
    private volatile boolean connected = false;

    public NettyConnection(String name, URI configUri, ECSChunkStorageConfig config) {
        this.name = name;
        this.socketAddress = InetSocketAddress.createUnresolved(configUri.getHost(), configUri.getPort());
        this.configUri = configUri;
        this.config = config;
    }

    public boolean connect() {
        if(connected) return true;

        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Http2ClientInitializer initializer = new Http2ClientInitializer(null, Integer.MAX_VALUE, socketAddress.getHostName(),
                    socketAddress.getPort());
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.remoteAddress(socketAddress.getHostName(), socketAddress.getPort());
            b.handler(initializer);

            channel = b.connect()
                    .syncUninterruptibly()
                    .channel();

            http2SettingsHandler = initializer.getSettingsHandler();
            http2SettingsHandler.awaitSettings(60, TimeUnit.SECONDS);

            log.info("Connected to [ {} : {} ]", socketAddress.getHostName(), socketAddress.getPort());
            connected = true;
            return true;
        } catch (Throwable t) {
            log.error("met exception", t);
        }
        return false;
    }

    @Override
    public boolean putObject(String bucket, String key, long offset, int length, InputStream data) {
        try {
            String streamId = bucket + key;
            byte[] content = new byte[length];
            data.readNBytes(content, 0, length);
            var request = new DefaultFullHttpRequest(HttpVersion.valueOf("HTTP/2.0"), HttpMethod.PUT, "/" + bucket + "/" + key, Unpooled.wrappedBuffer(content));
            request.headers()
                    .set(HttpHeaderNames.HOST, configUri)
                    .set(HttpHeaderNames.ACCEPT, "*/*")
                    .set(HttpHeaderNames.CONTENT_LENGTH, length)
                    .set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream")
                    .set(config.HeaderEMCExtensionIndexGranularity, config.indexGranularity)
                    .set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), HttpScheme.HTTPS)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.DEFLATE);
            responseHandler.put(streamId, channel.write(request), channel.newPromise());
            channel.writeAndFlush(request);
            var response = responseHandler.awaitResponse(streamId, 60, TimeUnit.SECONDS);
            return response.status() == HttpResponseStatus.OK;
        } catch (Exception e) {
            log.error("put object {} {} content {} failed", bucket, key, length, e);
            if (!channel.isOpen()) {
                close();
                connect();
            }
        }
        return false;
    }

    @Override
    public boolean putObject(String bucket, String key, InputStream data) {
        try {
            String streamId = bucket + key;
            byte[] content =  data.readAllBytes();
            var request = new DefaultFullHttpRequest(HttpVersion.valueOf("HTTP/2.0"), HttpMethod.PUT, "/" + bucket + "/" + key, Unpooled.wrappedBuffer(content));
            request.headers()
                    .set(HttpHeaderNames.HOST, configUri)
                    .set(HttpHeaderNames.ACCEPT, "*/*")
                    .set(HttpHeaderNames.CONTENT_LENGTH, content.length)
                    .set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream")
                    .set(config.HeaderEMCExtensionIndexGranularity, config.indexGranularity)
                    .set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), HttpScheme.HTTPS)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.DEFLATE);
            responseHandler.put(streamId, channel.write(request), channel.newPromise());
            channel.writeAndFlush(request);
            var response = responseHandler.awaitResponse(streamId, 60, TimeUnit.SECONDS);
            return response.status() == HttpResponseStatus.OK;
        } catch (Exception e) {
            log.error("put object {} {} failed", bucket, key, e);
            if (!channel.isOpen()) {
                close();
                connect();
            }
        }
        return false;
    }

    public boolean putObject(String bucket, String key, byte[] content, String contentMd5, String expectETag) {
        try {
            String streamId = bucket + key;
            var request = new DefaultFullHttpRequest(HttpVersion.valueOf("HTTP/2.0"), HttpMethod.PUT, "/" + bucket + "/" + key,
                    Unpooled.wrappedBuffer(content));
            request.headers()
                    .set(HttpHeaderNames.HOST, configUri)
                    .set(HttpHeaderNames.ACCEPT, "*/*")
                    .set(HttpHeaderNames.CONTENT_LENGTH, content.length)
                    .set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream")
                    .set(HttpHeaderNames.CONTENT_MD5, contentMd5)
                    .set(config.HeaderEMCExtensionIndexGranularity, config.indexGranularity)
                    .set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), HttpScheme.HTTPS)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.DEFLATE);
            responseHandler.put(streamId, channel.write(request), channel.newPromise());
            channel.writeAndFlush(request);
            var response = responseHandler.awaitResponse(streamId, 60, TimeUnit.SECONDS);
            var resETag = response.headers().get("ETag");
            return response.status() == HttpResponseStatus.OK && (resETag == null || resETag.equals(expectETag));
        } catch (Exception e) {
            log.error("put object {} {} content {} failed", bucket, key, content.length, e);
            if (!channel.isOpen()) {
                close();
                connect();
            }
        }
        return false;
    }

    public boolean putObject(String bucket, String key, byte[] objectContent, long objectSize) {
        try {
            responseFuture = new CompletableFuture<>();
            var request = new DefaultFullHttpRequest(HttpVersion.valueOf("HTTP/2.0"), HttpMethod.PUT, "/" + bucket + "/" + key, Unpooled.wrappedBuffer(objectContent));
            request.headers()
                    .set(HttpHeaderNames.HOST, configUri)
                    .set(HttpHeaderNames.ACCEPT, "*/*")
                    .set(HttpHeaderNames.CONTENT_LENGTH, objectContent.length)
                    .set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream")
                    .set(config.HeaderEMCExtensionIndexGranularity, config.indexGranularity)
                    .set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), HttpScheme.HTTPS)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.DEFLATE);

            channel.writeAndFlush(request);
            var response = responseFuture.get(30, TimeUnit.SECONDS);
            return response.status() == HttpResponseStatus.OK;
        } catch (Exception e) {
            log.error("put object {} {} content {} failed", bucket, key, objectSize, e);
            if (!channel.isOpen()) {
                close();
                connect();
            }
        }
        return false;
    }

    @Override
    public boolean getObject(String bucket, String key, long fromOffset, int length, byte[] buffer, int bufferOffset) {
        return false;
    }

    public boolean getObject(String bucket, String key, long offset, byte[] buffer) {
        throw new UnsupportedOperationException("Netty connection is not support getObject");
    }


    public boolean deleteObject(String bucket, String key) {
        throw new UnsupportedOperationException("Netty connection is not support deleteObject");
    }

    public void close() {
        try {
            this.channel.close().sync();
        } catch (InterruptedException e) {
            log.error("close conn {} failed", name, e);
        }
    }


    public Pair<List<S3Object>, String> listObject(String s, Integer index, String bucket, Integer ctIndex) {
        throw new UnsupportedOperationException("Netty connection is not support listObject");
    }

    @Override
    public String name() {
        return null;
    }


    public void complete(HttpResponse response) {
        if (responseFuture != null) {
            responseFuture.complete(response);
        } else {
            log.error("unknown response {}", response.toString());
        }
    }
}
