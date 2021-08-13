/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.user.stream;

import com.google.common.collect.Streams;
import io.pravega.cli.user.utils.BackgroundConsoleListener;
import io.pravega.cli.user.Command;
import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.Timer;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;

import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Commands relating to Streams.
 */
public abstract class StreamCommand extends Command {
    static final String COMPONENT = "stream";

    protected StreamCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    private static Command.CommandDescriptor.CommandDescriptorBuilder createDescriptor(String name, String description) {
        return Command.CommandDescriptor.builder()
                .component(COMPONENT)
                .name(name)
                .description(description);
    }

    //region Create

    public static class Create extends StreamCommand {
        public Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
            @Cleanup
            val streamManager = StreamManager.create(getClientConfig());
            val streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.builder()
                            .scaleType(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS)
                            .minNumSegments(getConfig().getDefaultSegmentCount())
                            .build())
                    .build();
            for (int i = 0; i < getCommandArgs().getArgs().size(); i++) {
                val s = getScopedNameArg(i);
                val success = streamManager.createStream(s.getScope(), s.getName(), streamConfiguration);
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

    //region Delete

    public static class Delete extends StreamCommand {
        public Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
            @Cleanup
            val sm = StreamManager.create(getClientConfig());
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

    public static class List extends StreamCommand {
        public List(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureArgCount(1);
            @Cleanup
            val sm = StreamManager.create(getClientConfig());
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

    //region Append

    public static class Append extends StreamCommand {
        public Append(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() throws Exception {
            ensureArgCount(2, 3);
            val scopedStream = getScopedNameArg(0);
            String routingKey = null;
            int eventCount;
            if (getCommandArgs().getArgs().size() == 3) {
                routingKey = getArg(1);
                eventCount = getIntArg(2);
            } else {
                eventCount = getIntArg(1);
            }

            @Cleanup
            val eventStreamClientFactory = EventStreamClientFactory.withScope(scopedStream.getScope(), getClientConfig());
            @Cleanup
            val writer = eventStreamClientFactory.createEventWriter(scopedStream.getName(), new UTF8StringSerializer(), EventWriterConfig.builder().build());

            String eventPrefix = UUID.randomUUID().toString();
            output("Appending %s Event(s) with payload prefix '%s' having routing key '%s'.", eventCount, eventPrefix, routingKey);
            val futures = new CompletableFuture[eventCount];
            for (int i = 0; i < eventCount; i++) {
                if (routingKey == null) {
                    futures[i] = writer.writeEvent(String.format("%s_%s", eventPrefix, i));
                } else {
                    futures[i] = writer.writeEvent(routingKey, String.format("%s_%s", eventPrefix, i));
                }
            }

            CompletableFuture.allOf(futures).get(getConfig().getTimeoutMillis(), TimeUnit.MILLISECONDS);
            output("Done.");
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("append", "Appends a number of Events to a Stream.")
                    .withArg("scoped-stream-name", "Scoped Stream name to append.")
                    .withArg("[routing-key]", "(Optional) Routing key to use.")
                    .withArg("event-count", "Number of events to append.")
                    .withSyntaxExample("scope1/stream1 100", "Appends 100 events to 'scope1/stream1'.")
                    .withSyntaxExample("scope1/stream1 \"my routing key\"100", "Appends 100 events to 'scope1/stream1' with routing key 'my routing key'.")
                    .build();
        }
    }

    //endregion

    //region Read

    public static class Read extends StreamCommand {
        public Read(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureArgCount(1, 2, 3);
            val scopedStream = getScopedNameArg(0);
            boolean group = false;
            long timeoutInMillis = Long.MAX_VALUE;
            if (getCommandArgs().getArgs().size() == 3) {
                group = getBooleanArg(1);
                timeoutInMillis = Long.parseLong(getArg(2)) * 1000;
            } else if (getCommandArgs().getArgs().size() == 2) {
                if (isBooleanArg(1)) {
                    group = getBooleanArg(1);
                } else {
                    timeoutInMillis = getLongArg(1) * 1000;
                }
            }
            final Aggregator aggregator = group ? new GroupedItems() : new SingleItem();

            @Cleanup
            val listener = new BackgroundConsoleListener();
            listener.start();

            val readerGroup = UUID.randomUUID().toString().replace("-", "");
            val readerId = UUID.randomUUID().toString().replace("-", "");

            val clientConfig = getClientConfig();
            val readerConfig = ReaderConfig.builder().build();
            @Cleanup
            val factory = EventStreamClientFactory.withScope(scopedStream.getScope(), clientConfig);
            @Cleanup
            val rgManager = ReaderGroupManager.withScope(scopedStream.getScope(), clientConfig);
            val rgConfig = ReaderGroupConfig.builder().stream(scopedStream.toString()).build();
            rgManager.createReaderGroup(readerGroup, rgConfig);
            try (val reader = factory.createReader(readerId, readerGroup, new UTF8StringSerializer(), readerConfig)) {
                EventRead<String> event;
                int displayCount = 0;
                Timer timer = new Timer();
                while (!listener.isTriggered() && timer.getElapsedMillis() < timeoutInMillis && (event = reader.readNextEvent(2000)) != null) {
                    if (event.getEvent() == null) {
                        // Nothing read yet.
                        aggregator.flush();
                        continue;
                    }
                    boolean accepted = aggregator.accept(event);
                    if (accepted && ++displayCount > getConfig().getMaxListItems()) {
                        aggregator.flush();
                        output("Reached maximum number of events %s. Change this using '%s' config value.",
                                getConfig().getMaxListItems(), InteractiveConfig.MAX_LIST_ITEMS);
                        break;
                    }
                }
                aggregator.flush();
            } finally {
                rgManager.deleteReaderGroup(readerGroup);
                listener.stop();
            }
            output("Done.");
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("read", "Reads all Events from a Stream and then tails the Stream.")
                    .withArg("scoped-stream-name", "Scoped Stream name to read from.")
                    .withArg("[group-similar]", "(Optional). If set ('true'), displays a count of events per prefix (as generated using 'stream append').")
                    .withArg("[timeout-in-seconds]", "(Optional). If set (>=0), reads events up to the specified timeout in seconds.")
                    .withSyntaxExample("scope1/stream1", "Reads and displays all events in 'scope1/stream1'.")
                    .withSyntaxExample("scope1/stream1 true", "Reads all events in `scope1/stream1' and displays a summary.")
                    .build();
        }

        private static abstract class Aggregator {
            abstract boolean accept(EventRead<String> event);

            abstract void flush();
        }

        private class SingleItem extends Aggregator {
            @Override
            boolean accept(EventRead<String> event) {
                output("\t%s", event.getEvent());
                return true;
            }

            @Override
            void flush() {
                // Nothing to do.
            }
        }

        private class GroupedItems extends Aggregator {
            private String lastGroup = null;
            private int count = 0;

            @Override
            boolean accept(EventRead<String> event) {
                if (event.getEvent() == null) {
                    return false;
                }
                int pos = event.getEvent().indexOf("_");
                val eventGroup = pos < 0 ? event.getEvent() : event.getEvent().substring(0, pos);
                if (eventGroup.equals(this.lastGroup)) {
                    this.count++;
                    return false;
                } else {
                    flush();
                    this.lastGroup = eventGroup;
                    this.count = 1;
                    return true;
                }
            }

            @Override
            void flush() {
                if (this.lastGroup != null) {
                    output("\t%s: %s events.", this.lastGroup, this.count);
                    this.lastGroup = null;
                    this.count = 0;
                }
            }
        }
    }

    //endregion
}
