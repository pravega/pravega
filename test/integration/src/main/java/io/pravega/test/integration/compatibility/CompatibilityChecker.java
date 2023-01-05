package io.pravega.test.integration.compatibility;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

// write events
// read events,
// execute various Stream API operations (create, update, seal, delete)
// create/commit/abort transactions,
// use K/V Tables,

// Large Event API,
// Stream Tags Api,
// manage some StreamCuts,
// use ByteAPI,
// watermarks,
// and batch API
@Slf4j
public class CompatibilityChecker {
    public  URI controllerURI;
    public  StreamManager streamManager;
    public  String scopeName = "Compatibility-test-scope";
    public String streamName = "Compatibility-test-stream";
    public EventStreamClientFactory clientFactory ;
    private static final int READER_TIMEOUT_MS = 2000;
    public StreamConfiguration streamConfig;

    public  void setUp(){
        controllerURI = URI.create("tcp://localhost:9090");
        streamManager = StreamManager.create(controllerURI);
        streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        clientFactory = EventStreamClientFactory.withScope(scopeName,ClientConfig.builder().controllerURI(controllerURI).build());
    }
    public void writeAndReadEvent(){
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, streamConfig);
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,new UTF8StringSerializer(), EventWriterConfig.builder().build());
        String readerGroupId = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);


        EventStreamReader<String> reader =  clientFactory.createReader("reader-1", readerGroupId, new UTF8StringSerializer(), ReaderConfig.builder().build());
        // Writing Event to the stream
        for( int event = 0; event < 10; event++)
            writer.writeEvent("event test" + event);
        log.info("Reading all the events from {}, {}", scopeName, streamName);
        EventRead<String> event = null;
        do {
            try {
                event = reader.readNextEvent(READER_TIMEOUT_MS);
                if (event.getEvent() != null) {
                    System.out.format("Read event '%s'%n", event.getEvent());
                }
            } catch (ReinitializationRequiredException e) {
                //There are certain circumstances where the reader needs to be reinitialized
                e.printStackTrace();
            }
        } while (event.getEvent() != null);
        log.info("No more events from {}, {}", scopeName, streamName);

    }

    public void streamAPIOps() throws DeleteScopeFailedException {
        // create stream
        streamManager.createStream(scopeName, streamName, streamConfig);
        String readerGroupId = UUID.randomUUID().toString().replace("-", "");

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupId);

        StreamCut streamCut =  readerGroup.getStreamCuts().get(Stream.of(scopeName, streamName));

        // Truncate Stream
        if ( streamManager.truncateStream(scopeName, streamName, streamCut) )
            log.info("Stream: {} is Truncated.", streamName);

        // Update Stream
        streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build();
        if(streamManager.updateStream(scopeName, streamName, streamConfig))
            log.info("Stream: {} is Updated ", streamName);


        // Checking scope is exist
        if(streamManager.checkScopeExists(scopeName))
            log.info("Scope: {} is currently exist", scopeName);
        // Checking stream is exist
        if(streamManager.checkStreamExists(scopeName, streamName))
            log.info("Stream: {} is currently exist", streamName);
        // Seal Stream
        if(streamManager.sealStream(scopeName, streamName))
            log.info("Stream: {} is currently sealed", streamName);
        // Delete Stream
        streamManager.deleteStream(scopeName, streamName);
        if(!streamManager.checkStreamExists(scopeName, streamName))
            log.info("Stream: {} is delete successfully", streamName);
        // Delete Scope
        streamManager.deleteScopeRecursive(scopeName);
        if(!streamManager.checkScopeExists(scopeName))
            log.info("Scope: {} is deleted successfully", scopeName);
    }

    public static void main(String[] args) throws DeleteScopeFailedException {
        CompatibilityChecker compatibilityChecker = new CompatibilityChecker();
        compatibilityChecker.setUp();
        compatibilityChecker.writeAndReadEvent();
        compatibilityChecker.streamAPIOps();
        System.out.println("hello this is test");
    }
}
