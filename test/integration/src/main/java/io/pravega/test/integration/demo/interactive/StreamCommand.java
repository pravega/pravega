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

    private static CommandDescriptor createDescriptor(String name, String description, ArgDescriptor... args) {
        return new CommandDescriptor(COMPONENT, name, description, args);
    }

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
            return createDescriptor("create", "Creates one or more Streams.",
                    new ArgDescriptor("scoped-stream-names", "Names of the Scoped Streams to create."));
        }
    }

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
            return createDescriptor("delete", "Deletes one or more Streams.",
                    new ArgDescriptor("scoped-stream-names", "Names of the Scoped Streams to delete."));
        }
    }

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
            return createDescriptor("list", "Lists all Streams in a Scope.",
                    new ArgDescriptor("scope-name", "Name of Scope to list Streams from."));
        }
    }
}
