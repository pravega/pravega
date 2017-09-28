package io.pravega.local;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.NotImplementedException;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
public class PravegaInputOperator
{
    private Sequence startingPosition;
    private long currentWindowId;
    private long readerTimeoutMs=10000;
    private String scope;
    private String streamName;
    private String readerName;
    private String readerGroupName;
    private ReaderGroup readerGroup;
    private StreamManager streamManager;
    private transient EventStreamReader<String> eventStreamReader;
    private transient final Logger logger = LoggerFactory.getLogger(PravegaInputOperator.class);
    private URI controllerUri;
    private ClientFactory clientFactory;
    private Serializer<String> serializer = new JavaSerializer<String>();
    public PravegaInputOperator() throws URISyntaxException {
        this.scope = "test-scope";
        this.streamName = "test-stream";
        this.readerGroupName = "test-group";
        this.readerName = "test-reader";
        this.controllerUri = new URI("tcp://localhost:9090");
    }
    public void setup() {
        setupStreamManager();
        setupScope();
        setupStream();
        setupReaderGroup();
        setupReader();
    }
    private void setupStream()
    {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1)) // TODO: Make configurable
                .build();
        // TODO: What happens if we pass a more scaled stream in config than the existing
        // TODO: Log whether new or existing stream is used
        boolean streamIsNew = this.streamManager.createStream(scope, streamName, streamConfig);
    }
    private void setupScope()
    {
        boolean scopeIsNew = this.streamManager.createScope(scope);
        if (scopeIsNew) {
            logger.debug("Creating new scope {}", scope);
        } else {
            logger.debug("Using existing scope {}", scope);
        }
    }
    private void setupStreamManager()
    {
        this.streamManager = StreamManager.create(controllerUri);
    }
    private void setupReaderGroup()
    {
        ReaderGroupConfig config = ReaderGroupConfig.builder()
                .startingPosition(this.startingPosition)
                .build();
        logger.info("Setting up reader group");
        try (ReaderGroupManager manager = ReaderGroupManager.withScope(scope, controllerUri)) {
            manager.createReaderGroup(readerGroupName, config, Collections.singleton(streamName));
            this.readerGroup = manager.getReaderGroup(readerGroupName);
        }
        logger.info("Reader group set up");
    }
    private void setupReader()
    {
        ReaderConfig readerConfig = ReaderConfig.builder()
                // .initialAllocationDelay() // TODO: This need to be configured?
                .build();
        // create client
        logger.info("Setting up reader");
        this.clientFactory = ClientFactory.withScope(scope, controllerUri);
        this.eventStreamReader = clientFactory.createReader(
                readerName,
                readerGroupName,
                serializer,
                readerConfig);
        logger.info("Reader set up");
    }
    public void triggerCheckpoint() {
        logger.info("Going for checkpoint");
        this.readerGroup.initiateCheckpoint("checkpoint", Executors.newScheduledThreadPool(5));
        logger.info("Left checkpoint");
    }
    public void emitTuples()
    {
        try {
            logger.info("Going to read");
            EventRead<String> eventRead = this.eventStreamReader.readNextEvent(10000);
            String event = eventRead.getEvent();
            if (eventRead.isCheckpoint()) {
                logger.info("Received checkpoint event");
                // TODO: Handle this case
            }
            if (event != null) {
                System.out.println(event);
            }
        } catch (ReinitializationRequiredException e) {
            // TODO: Handle this
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) {
        try {
            final PravegaInputOperator pravegaInputOperator = new PravegaInputOperator();
            pravegaInputOperator.setup();
            pravegaInputOperator.emitTuples();
            new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(2000);
                        pravegaInputOperator.triggerCheckpoint();
                    } catch (InterruptedException e) {
                    }
                }
            }.start();
        } catch (URISyntaxException e) {
        }
    }
}