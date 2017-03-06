/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc;


import com.emc.pravega.controller.stream.api.v1.ControllerService;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * Thrift based RPC server implementation. (Initial version)
 */
@Slf4j
public class RPCServer {

    public static void start(final ControllerService.AsyncIface controllerService, final RPCServerConfig serverConfig) {
        Runnable simple = () -> {
            try {
                threadedSelectorServer(new ControllerService.AsyncProcessor<>(controllerService), serverConfig);
            } catch (TTransportException e) {
                throw Lombok.sneakyThrow(e);
            }
        };
        new Thread(simple).start();
    }

    private static void threadedSelectorServer(final TProcessor processor,
                                               final RPCServerConfig serverConfig) throws TTransportException {
        TNonblockingServerSocket socket = new TNonblockingServerSocket(serverConfig.getPort());

        TThreadedSelectorServer.Args config = new TThreadedSelectorServer.Args(socket);
        config.processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .protocolFactory(new TBinaryProtocol.Factory())
                .workerThreads(serverConfig.getWorkerThreadCount())
                .selectorThreads(serverConfig.getSelectorThreadCount());
        config.maxReadBufferBytes = serverConfig.getMaxReadBufferBytes();
        TServer server = new TThreadedSelectorServer(config);
        log.info("Starting Controller Server (Threaded Selector Server) on port {}", serverConfig.getPort());
        server.serve();
    }
}
