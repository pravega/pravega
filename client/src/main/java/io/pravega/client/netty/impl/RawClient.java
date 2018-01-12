package io.pravega.client.netty.impl;

import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawClient implements AutoCloseable {

    private final CompletableFuture<ClientConnection> connection;
    
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<Reply>> requests = new HashMap<>();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    
    private final class ResponseProcessor extends FailingReplyProcessor {; 
        
        @Override
        public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
            log.debug("Received stream segment info {}", streamInfo);
            reply(streamInfo);
        }        

        @Override
        public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {
            log.debug("Received stream segment attribute {}", segmentAttribute);
            reply(segmentAttribute);
        }
        
        @Override
        public void segmentAttributeUpdated(SegmentAttributeUpdated segmentAttributeUpdated) {
            log.debug("Received stream segment attribute update result {}", segmentAttributeUpdated);
            reply(segmentAttributeUpdated);
        }
        
        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            failRequest(wrongHost.getRequestId(), new ConnectionFailedException(wrongHost.toString()));
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            failRequest(noSuchSegment.getRequestId(), new ConnectionFailedException(noSuchSegment.toString()));
        }
        
        @Override
        public void connectionDropped() {
            closeConnection(new ConnectionFailedException());
        }

        @Override
        public void processingFailure(Exception error) {
            log.warn("Processing failure: ", error);
            closeConnection(error);
        }
    }
    
    RawClient(ConnectionFactory connectionFactory, PravegaNodeUri endpoint) {
        connection = connectionFactory.establishConnection(endpoint, responseProcessor);
    }
    
    private void failRequest(long requestId, Exception e) {
        CompletableFuture<Reply> future;
        synchronized (lock) {
            future = requests.remove(requestId);
        }
        if (future != null) {
            future.completeExceptionally(e);
        } 
    }

    private void reply(Reply reply) {
        CompletableFuture<Reply> future;
        synchronized (lock) {
            future = requests.remove(reply.getRequestId());
        }
        if (future != null) {
            future.complete(reply);
        }
    }
    
    private void closeConnection(Throwable exceptionToInflightRequests) {
        log.info("Closing connection with exception: {}", exceptionToInflightRequests.getMessage());
        connection.thenAccept(c -> {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ", e);
            }
        });
        failAllInflight(exceptionToInflightRequests);
    }
    
    private void failAllInflight(Throwable e) {
        log.info("SegmentMetadata connection failed due to a {}.", e.getMessage());
        List<CompletableFuture<Reply>> requestsToFail;
        synchronized (lock) {
            requestsToFail = new ArrayList<>(requests.values());
            requests.clear();
        }
        for (CompletableFuture<Reply> request : requestsToFail) {
            request.completeExceptionally(e);
        }
    }
    
    public CompletableFuture<Reply> sendRequest(long requestId, WireCommand request) {
        log.debug("Sending request: {}", request);
        ClientConnection c = connection.join();
        CompletableFuture<Reply> reply = new CompletableFuture<>();
        synchronized (lock) {
            requests.put(requestId, reply);
        }
        try {
            c.send(request);
        } catch (ConnectionFailedException e) {
            reply.completeExceptionally(e);
        }
        return reply;
    }
    
    @Override
    public void close() {
        closeConnection(new ConnectionClosedException());
    }

}
