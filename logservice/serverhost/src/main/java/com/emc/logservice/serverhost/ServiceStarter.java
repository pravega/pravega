package com.emc.logservice.serverhost;

import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.server.mocks.InMemoryServiceBuilder;
import com.emc.logservice.server.service.ServiceBuilder;
import com.emc.logservice.serverhost.handler.LogServiceConnectionListener;

import java.time.Duration;

/**
 * Starts the Log Service.
 */
public final class ServiceStarter {
    private static final int Port = 12345;
    private static final int ContainerCount = 1;
    private static final Duration InitializeTimeout = Duration.ofSeconds(30);

    public static void main(String[] args) {
        System.out.println("Initializing ServiceBuilder ...");
        try (ServiceBuilder serviceBuilder = new InMemoryServiceBuilder(ContainerCount)) {
            System.out.println("Initializing Container Manager ...");
            serviceBuilder.getContainerManager().initialize(InitializeTimeout).join();
            System.out.println("Creating StreamSegmentService ...");
            StreamSegmentStore service = serviceBuilder.createStreamSegmentService();
            System.out.println("Starting LogServiceConnectionListener ...");
            try (LogServiceConnectionListener listener = new LogServiceConnectionListener(false, Port, service)) {
                listener.startListening();
                System.out.println("LogServiceConnectionListener started successfully.");
                Thread.sleep(600000);
            }

            System.out.println("LogServiceConnectionListener is now closed.");
        }
        catch (InterruptedException ex) {
            System.err.println(ex);
        }

        System.out.println("StreamSegmentService is now closed.");
    }
}
