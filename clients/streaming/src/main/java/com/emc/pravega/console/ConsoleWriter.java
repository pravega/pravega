/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */


package com.emc.pravega.console;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ConsoleWriter {
    static final AtomicBoolean STOP = new AtomicBoolean(false);

    private String scope;
    private String stream;
    private URI controller;
    private StreamManager manager;
    private ClientFactory clientFactory;
    private EventStreamWriter writer;

    void configure(OptionSet options) throws URISyntaxException {
        this.scope = (String) options.valueOf("scope");
        this.stream = (String) options.valueOf("stream");
        this.controller = new URI((String) options.valueOf("controller"));
        this.manager = StreamManager.withScope(this.scope, this.controller);
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
        // Determine scaling policy
        if (options.has("fixed")) {
            scalingPolicy = ScalingPolicy.fixed((Integer) options.valueOf("segments"));
        } else if (options.has("scaleByEvents")) {
            scalingPolicy = ScalingPolicy.byEventRate((Integer) options.valueOf("rate"), 1, 1);
        } else if (options.has("scaleByBytes")) {
            ScalingPolicy.byDataRate((Integer) options.valueOf("byteRate"), 1, 1);
        }

        this.manager.createScope();
        this.manager.createStream(stream, StreamConfiguration.builder()
                                                             .scope(this.scope)
                                                             .streamName(this.stream)
                                                             .scalingPolicy(scalingPolicy)
                                                             .build());
        this.clientFactory = ClientFactory.withScope(scope, controller);
        this.writer = clientFactory.createEventWriter(stream,
                                                      new JavaSerializer<String>(),
                                                      EventWriterConfig.builder().build());
    }

    void write(String message) {
        this.writer.writeEvent(message);
        this.writer.flush();
    }

    public static void main(String[] args) {
        OptionSet options = null;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                STOP.set(true);
                log.info("Closing writer");
            }
        });
        // Parse command line arguments
        OptionParser parser = new OptionParser() {
            {
                accepts("scope", "Scope name").withRequiredArg().ofType(String.class).required().describedAs("name");
                accepts("stream", "Stream name").withRequiredArg().ofType(String.class).required().describedAs("name");
                accepts("controller", "Controller URI").withRequiredArg()
                        .ofType(String.class)
                        .required()
                        .describedAs("Controller URI");
                accepts("fixed", "Use fixed scaling policy");
                accepts("segments", "Number of segments for fixed policy")
                        .requiredIf("fixed")
                        .withRequiredArg().
                        ofType(Integer.class).
                        describedAs("# of segments");
                accepts("scaleByEvents", "Scaling policy is by events");
                accepts("eventRate", "Event rate for event-based policy")
                        .requiredIf("scaleByEvents")
                        .withRequiredArg().ofType(Integer.class)
                        .describedAs("event rate");
                accepts("scaleByBytes", "Scaling policy is by bytes");
                accepts("byteRate", "Byte rate for volume-based policy")
                        .requiredIf("scaleByBytes")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .describedAs("byte rate");
                accepts("help").forHelp();
            }
        };

        try {
            // Parse options
            options = parser.parse(args);
        } catch (Exception e) {
            try {
                parser.printHelpOn( System.out );
            } catch (IOException ioe) {
                log.warn("Exception while printing help");
            } finally {
                System.exit(1);
            }
        }

        try {
            // If asked for config help, then print and leave
            if (options.has("help")) {
                parser.printHelpOn(System.out);
            } else {
                // Create console writer
                ConsoleWriter writer = new ConsoleWriter();
                writer.configure(options);
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Ready to write messages");
                while (!STOP.get()) {
                    String message = br.readLine();
                    if (message != null) {
                        writer.write(message);
                    }
                }
            }
        } catch (URISyntaxException e) {
            log.error("Failed to get controller URI", e);
        } catch (IOException e) {
            log.error("Failed to read line", e);
        }
    }
}
