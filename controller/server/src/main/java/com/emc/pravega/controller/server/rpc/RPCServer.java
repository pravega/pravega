/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc;


import com.emc.pravega.controller.stream.api.v1.ControllerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import static com.emc.pravega.controller.util.Config.SERVER_MAX_READ_BUFFER_BYTES;
import static com.emc.pravega.controller.util.Config.SERVER_PORT;
import static com.emc.pravega.controller.util.Config.SERVER_SELECTOR_THREAD_COUNT;
import static com.emc.pravega.controller.util.Config.SERVER_WORKER_THREAD_COUNT;

/**
 * Thrift based RPC server implementation. (Initial version)
 */
@Slf4j
public class RPCServer {

    public static void start(final ControllerService.AsyncIface controllerService, final int port) {
        try {

            Runnable simple = () -> threadedSelectorServer(new ControllerService.AsyncProcessor<>(controllerService), port);

            new Thread(simple).start();
        } catch (Exception x) {
            log.error("Exception during start of RPC Server", x);
        }
    }

    public static void start(ControllerService.AsyncIface controllerService) {
        try {

            Runnable simple = () -> threadedSelectorServer(new ControllerService.AsyncProcessor<>(controllerService), SERVER_PORT);

            new Thread(simple).start();
        } catch (Exception x) {
            log.error("Exception during start of RPC Server", x);
        }
    }

    private static void simpleThreaded(final TMultiplexedProcessor processor) {
        try {
            log.info("Listening on port " + SERVER_PORT);
            TServerTransport serverTransport = new TServerSocket(SERVER_PORT);
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            log.info("Starting Controller Server (ThreadPool based)...");
            server.serve();
        } catch (Exception e) {
            log.error("Exception during start of ThreadPool based RPC server");
        }
    }

    private static void threadedSelectorServer(final TProcessor processor, int port) {
        try {
            TNonblockingServerSocket socket = new TNonblockingServerSocket(port);

            TThreadedSelectorServer.Args config = new TThreadedSelectorServer.Args(socket);
            config.processor(processor)
                    .transportFactory(new TFramedTransport.Factory())
                    .protocolFactory(new TBinaryProtocol.Factory())
                    .workerThreads(SERVER_WORKER_THREAD_COUNT)
                    .selectorThreads(SERVER_SELECTOR_THREAD_COUNT);
            config.maxReadBufferBytes = SERVER_MAX_READ_BUFFER_BYTES;
            TServer server = new TThreadedSelectorServer(config);
            log.info("Starting Controller Server (Threaded Selector Server) on port {}", port);
            server.serve();
        } catch (TTransportException e) {
            log.error("Exception during start of Threaded Selector Server", e);
        }
    }
}
