/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo.interactive;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import java.net.URI;
import java.util.Comparator;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;

abstract class StreamCommand extends Command {
    static final String COMPONENT = "stream";

    StreamCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    private static Command.CommandDescriptor.CommandDescriptorBuilder createDescriptor(String name, String description) {
        return Command.CommandDescriptor.builder()
                .component(COMPONENT)
                .name(name)
                .description(description);
    }

    //region Create

    static class Create extends StreamCommand {
        Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            Preconditions.checkArgument(getCommandArgs().getArgs().size() > 0, "At least one stream name expected.");
            @Cleanup
            val sm = StreamManager.create(URI.create(getConfig().getControllerUri()));
            val sc = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.builder()
                            .scaleType(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS)
                            .minNumSegments(getConfig().getDefaultSegmentCount())
                            .build())
                    .build();
            for (int i = 0; i < getCommandArgs().getArgs().size(); i++) {
                val s = getScopedNameArg(i);
                val success = sm.createStream(s.getScope(), s.getName(), sc);
                if (success) {
                    output("Stream '%s/%s' created successfully.", s.getScope(), s.getName());
                } else {
                    output("Stream '%s/%s' could not be created.", s.getScope(), s.getName());
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates one or more Streams.")
                    .withArg("scoped-stream-names", "Names of the Scoped Streams to create.")
                    .withSyntaxExample("scope1/stream1 scope1/stream2 scope2/stream3", "Creates stream1 and stream2 in scope1 and stream3 in scope2.")
                    .build();
        }
    }

    //endregion

    static class Delete extends StreamCommand {
        Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            Preconditions.checkArgument(getCommandArgs().getArgs().size() > 0, "At least one stream name expected.");
            @Cleanup
            val sm = StreamManager.create(URI.create(getConfig().getControllerUri()));
            for (int i = 0; i < getCommandArgs().getArgs().size(); i++) {
                val s = getScopedNameArg(i);
                boolean sealed = sm.sealStream(s.getScope(), s.getName());
                if (sealed) {
                    output("Stream '%s/%s' has been sealed.", s.getScope(), s.getName());
                }

                val success = sm.deleteStream(s.getScope(), s.getName());
                if (success) {
                    output("Stream '%s/%s' deleted successfully.", s.getScope(), s.getName());
                } else {
                    output("Stream '%s/%s' could not be deleted.", s.getScope(), s.getName());
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("delete", "Deletes one or more Streams.")
                    .withArg("scoped-stream-names", "Names of the Scoped Streams to delete.")
                    .withSyntaxExample("scope1/stream1 scope1/stream2 scope2/stream3", "Deletes stream1 and stream2 from scope1 and stream3 from scope2.")
                    .build();
        }
    }

    //endregion

    //region List

    static class List extends StreamCommand {
        List(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureArgCount(1);
            @Cleanup
            val sm = StreamManager.create(URI.create(getConfig().getControllerUri()));
            val streamIterator = sm.listStreams(getArg(0));
            if (!streamIterator.hasNext()) {
                output("Scope '%s' does not have any Streams.", getArg(0));
            }

            Streams.stream(streamIterator)
                    .sorted(Comparator.comparing(Stream::getScopedName))
                    .forEach(stream -> output("\t%s", stream.getScopedName()));
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list", "Lists all Streams in a Scope.")
                    .withArg("scope-name", "Name of Scope to list Streams from.")
                    .build();
        }
    }

    //endregion
}
