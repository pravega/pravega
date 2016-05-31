package com.emc.logservice.serverhost;

import com.emc.logservice.server.*;
import com.emc.logservice.server.containers.StreamSegmentContainer;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;
import com.emc.logservice.server.logs.DurableLogFactory;
import com.emc.logservice.server.reading.ReadIndexFactory;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageabstraction.StorageFactory;

import java.io.PrintStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletionException;

/**
 * Interactive (command-line) StreamSegmentStore tester.
 */
public class InteractiveStreamSegmentStoreTester implements AutoCloseable {
    private final StreamSegmentContainer container;
    private final Duration DefaultTimeout = Duration.ofSeconds(30);
    private final PrintStream logger;
    private final PrintStream errorLogger;

    public InteractiveStreamSegmentStoreTester(DurableDataLogFactory dataLogFactory, StorageFactory storageFactory, PrintStream logger, PrintStream errorLogger) {
        MetadataRepository metadataRepository = new InMemoryMetadataRepository();
        OperationLogFactory durableLogFactory = new DurableLogFactory(dataLogFactory);
        CacheFactory cacheFactory = new ReadIndexFactory();
        this.logger = logger;
        this.errorLogger = errorLogger;
        this.container = new StreamSegmentContainer("test", metadataRepository, durableLogFactory, cacheFactory, storageFactory);
        log("Container '%s' created.", this.container.getId());
    }

    public void run() {
        // Initialize container.
        try {
            this.container.initialize(DefaultTimeout).join();
            log("Container '%s' initialized successfully.", this.container.getId());
        }
        catch (CompletionException ex) {
            log(ex, "Unable to initialize container '%s'.", this.container.getId());
            return;
        }

        // Start container.
        try {
            this.container.start(DefaultTimeout).join();
            log("Container '%s' started successfully.", this.container.getId());
        }
        catch (CompletionException ex) {
            log(ex, "Unable to start container '%s'.", this.container.getId());
            return;
        }

        try {
            /*
            create <name>
            delete <name>
            get <name>
            append <name> <string-data>
            read <name> <offset> <length>
            batch-create <parent-name>
            batch-merge <batch-name>
             */
        }
        finally {
            // Stop container upon exit
            this.container.stop(DefaultTimeout).join();
            log("Container '%s' stopped.", this.container.getId());
        }
    }

    @Override
    public void close() {
        try {
            this.container.close();
            log("Container '%s' closed.", this.container.getId());
        }catch(Exception ex){
            log(ex, "Unable to close container '%s'.", this.container.getId());
        }
    }

    private void log(String message, Object... args) {
        this.logger.println(String.format(message, args));
    }

    private void log(Exception ex, String message, Object... args) {
        this.errorLogger.println(String.format("ERROR: %s.", String.format(message, args)));
        getRealException(ex).printStackTrace(this.errorLogger);
    }

    private Throwable getRealException(Throwable ex) {
        if (ex instanceof CompletionException) {
            return ex.getCause();
        }

        return ex;
    }

    private static class InMemoryMetadataRepository implements MetadataRepository {
        private final HashMap<String, UpdateableContainerMetadata> metadatas = new HashMap<>();

        @Override
        public UpdateableContainerMetadata getMetadata(String streamSegmentContainerId) {
            synchronized (this.metadatas) {
                UpdateableContainerMetadata result = this.metadatas.getOrDefault(streamSegmentContainerId, null);
                if (result == null) {
                    result = new StreamSegmentContainerMetadata(streamSegmentContainerId);
                    this.metadatas.put(streamSegmentContainerId, result);
                }

                return result;
            }
        }
    }
}
