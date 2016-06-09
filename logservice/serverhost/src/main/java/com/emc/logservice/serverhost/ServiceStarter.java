package com.emc.logservice.serverhost;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.server.service.ServiceBuilder;
import com.emc.logservice.serverhost.handler.LogServiceConnectionListener;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Starts the Log Service.
 */
public final class ServiceStarter {
    private static final int Port = 12345;
    private static final int ContainerCount = 1;
    private static final Duration InitializeTimeout = Duration.ofSeconds(30);
    private final ServiceBuilder serviceBuilder;
    private LogServiceConnectionListener listener;
    private boolean closed;

    private ServiceStarter() {
        this.serviceBuilder = new DistributedLogServiceBuilder(ContainerCount);
        //this.serviceBuilder = new InMemoryServiceBuilder(ContainerCount);
    }

    private void start() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);

        System.out.println("Initializing Container Manager ...");
        this.serviceBuilder.getContainerManager().initialize(InitializeTimeout).join();

        System.out.println("Creating StreamSegmentService ...");
        StreamSegmentStore service = serviceBuilder.createStreamSegmentService();

        this.listener = new LogServiceConnectionListener(false, Port, service);
        listener.startListening();
        System.out.println("LogServiceConnectionListener started successfully.");
    }

    private void shutdown() {
        if (!this.closed) {
            this.serviceBuilder.close();
            System.out.println("StreamSegmentService is now closed.");

            this.listener.shutdown();
            System.out.println("LogServiceConnectionListener is now closed.");
            this.closed = true;
        }
    }

    public static void main(String[] args) {
        ServiceStarter serviceStarter = new ServiceStarter();
        try {
            serviceStarter.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        System.out.println("Caught interrupt signal...");
                        serviceStarter.shutdown();
                    }
                    catch (Exception e) {
                        // do nothing
                    }
                }
            });

            Thread.sleep(Long.MAX_VALUE);
        }
        catch (InterruptedException ex) {
            System.out.println("Caught interrupt signal");
        }
        finally {
            serviceStarter.shutdown();
        }
    }
}
