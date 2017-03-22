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

import java.net.URI;

public class ConsoleWriter {
    static volatile boolean stop = false;

    StreamManager manager;
    ClientFactory clientFactory;
    EventStreamWriter writer;

    ConsoleWriter(String scope, String stream, URI controller) {
        this.manager = StreamManager.withScope(scope, controller);
        this.manager.createScope();
        this.manager.createStream(stream, StreamConfiguration.builder()
                                                             .scope(scope)
                                                             .streamName(stream)
                                                             .scalingPolicy(ScalingPolicy.fixed(1))
                                                             .build());
        this.clientFactory = ClientFactory.withScope(scope, controller);
        this.writer = clientFactory.createEventWriter(stream,
                                                        new JavaSerializer<String>(),
                                                        new EventWriterConfig.builder().build());
    }

    void write(String message) {
        this.writer.writeEvent(message);
    }

    public static void main(String[] args) {
        String scope, stream;
        URI controller;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stop = true;
            }
        });
        // Parse command line arguments
        OptionParser parser = new OptionParser() {
            {
                accepts("scope").withRequiredArg().ofType(String.class).required();
                accepts("stream").withRequiredArg().ofType(String.class).required();
                accepts("controller").withRequiredArg().ofType(String.class).required();
                accepts("fixed");
                accepts("segments").requiredIf("fixed").withRequiredArg().ofType(Integer.class);
                accepts("scaleByEvents");
                accepts("rate").requiredIf("scaleByEvents").withRequiredArg().ofType(Integer.class);
                accepts("scaleByBytes");
                accepts("rate").requiredIf("scaleBybytes").withRequiredArg().ofType(Integer.class);
                accepts("help").forHelp();
            }
        };

        OptionSet options = parser.parse(args);

        scope = (String) options.valueOf("scope");
        stream = (String) options.valueOf("stream");
        controller = new URI((String) options.valueOf("controller"));

        // Create console writer
        ConsoleWriter writer = new ConsoleWriter(scope, stream, controller);
        while(!stop) {
            writer.write(System.console().readLine());
        }
    }
}
