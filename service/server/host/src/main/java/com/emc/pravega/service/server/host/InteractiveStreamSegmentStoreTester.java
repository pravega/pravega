/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Interactive (command-line) StreamSegmentStore tester.
 */
public class InteractiveStreamSegmentStoreTester {
    //region Members

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private final StreamSegmentStore streamSegmentStore;
    private final Duration defaultTimeout = Duration.ofSeconds(30);
    private final PrintStream out;
    private final PrintStream errorLogger;
    private final BufferedReader in;
    private final UUID clientId = UUID.randomUUID();
    private int appendCount;
    private int readCount;

    //endregion

    //region Constructor

    public InteractiveStreamSegmentStoreTester(ServiceBuilder serviceBuilder, InputStream in, PrintStream out, PrintStream errorLogger) {
        this.out = out;
        this.in = new BufferedReader(new InputStreamReader(in));
        this.errorLogger = errorLogger;
        this.streamSegmentStore = serviceBuilder.createStreamSegmentService();
    }

    //endregion

    //region Main Method

    public static void main(String[] args) {
        // Configure slf4j to not log anything (console or whatever). This interferes with the console interaction.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.TRACE);
        context.reset();

        ServiceBuilderConfig config;
        try {
            config = ServiceBuilderConfig.getConfigFromFile();
        } catch (IOException e) {
            System.out.println("Creation of ServiceBuilderConfig failed because of exception " + e);
            config = ServiceBuilderConfig.getDefaultConfig();
        }
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(config);
        serviceBuilder.withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::new)));

        try {
            serviceBuilder.initialize().join();
            InteractiveStreamSegmentStoreTester tester = new InteractiveStreamSegmentStoreTester(serviceBuilder, System.in, System.out, System.err);
            tester.run();
        } finally {
            serviceBuilder.close();
        }
    }

    //endregion

    //region Command Execution

    public void run() {
        log("InteractiveStreamStoreTester: ClientId = %s.", clientId);
        log("Available commands:");
        for (String syntax : Commands.SYNTAXES.values()) {
            log("\t%s", syntax);
        }
        try {
            while (true) {
                //this.out.print(">"); Commenting out since this interferes with printing read output.
                CommandLineParser commandParser = new CommandLineParser(this.in.readLine());
                String commandName = commandParser.getNext();
                if (commandName == null) {
                    break;
                }

                try {
                    switch (commandName) {
                        case Commands.CREATE:
                            createStream(commandParser);
                            break;
                        case Commands.DELETE:
                            deleteStream(commandParser);
                            break;
                        case Commands.SEAL:
                            sealStream(commandParser);
                            break;
                        case Commands.GET:
                            getStreamInfo(commandParser);
                            break;
                        case Commands.APPEND:
                            appendToStream(commandParser);
                            break;
                        case Commands.READ:
                            readFromStream(commandParser);
                            break;
                        case Commands.CREATE_TRANSACTION:
                            createTransaction(commandParser);
                            break;
                        case Commands.MERGE_TRANSACTION:
                            mergeTransaction(commandParser);
                            break;
                        default:
                            log("Unknown command '%s'.", commandName);
                            break;
                    }
                } catch (InvalidCommandSyntax ex) {
                    log(ex.getMessage());
                }
            }
        } catch (IOException ex) {
            log(ex, "Could not read from input.");
        }
    }

    private void createStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.SYNTAXES.get(Commands.CREATE));
        long startTime = getCurrentTime();
        await(this.streamSegmentStore.createStreamSegment(name, defaultTimeout), r -> log(startTime, "Created StreamSegment %s.", name));
    }

    private void deleteStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.SYNTAXES.get(Commands.DELETE));
        long startTime = getCurrentTime();
        await(this.streamSegmentStore.deleteStreamSegment(name, defaultTimeout), r -> log(startTime, "Deleted StreamSegment %s.", name));
    }

    private void sealStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.SYNTAXES.get(Commands.GET));
        long startTime = getCurrentTime();
        await(this.streamSegmentStore.sealStreamSegment(name, defaultTimeout), r -> log(startTime, "Sealed StreamSegment %s.", name));
    }

    private void getStreamInfo(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.SYNTAXES.get(Commands.GET));
        long startTime = getCurrentTime();
        await(this.streamSegmentStore.getStreamSegmentInfo(name, defaultTimeout), result ->
                log(startTime, "Name = %s, Length = %d, Sealed = %s, Deleted = %s.", result.getName(), result.getLength(), result.isSealed(), result.isDeleted()));
    }

    private void appendToStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        String data = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0 && data != null && data.length() > 0,
                Commands.SYNTAXES.get(Commands.APPEND));
        appendToStream(name, data.getBytes());
    }

    private void appendToStream(String name, byte[] data) {
        AppendContext context = new AppendContext(clientId, appendCount++);
        long startTime = getCurrentTime();
        await(this.streamSegmentStore.append(name, data, context, defaultTimeout), r -> log(startTime, "Appended %d bytes at offset %d.", data.length, r));
    }

    private void readFromStream(CommandLineParser parsedCommand) {
        // read name offset length
        String name = parsedCommand.getNext();
        int offset = parsedCommand.getNextOrDefault(Integer.MIN_VALUE);
        int length = parsedCommand.getNextOrDefault(Integer.MIN_VALUE);

        checkArguments(name != null && name.length() > 0 && offset >= 0 && length > 0, Commands.SYNTAXES.get(Commands.READ));

        final int readId = this.readCount++;
        log("Started Read #%d from %s.", readId, name);
        await(this.streamSegmentStore.read(name, offset, length, defaultTimeout), readResult ->
                CompletableFuture.runAsync(() -> {
                    while (readResult.hasNext()) {
                        ReadResultEntry entry = readResult.next();
                        try {
                            ReadResultEntryContents contents = entry.getContent().get();
                            byte[] rawData = new byte[contents.getLength()];
                            StreamHelpers.readAll(contents.getData(), rawData, 0, rawData.length);
                            String data = new String(rawData);
                            log("Read #%d (Offset=%d, Remaining=%d): %s", readId, entry.getStreamSegmentOffset(), readResult.getMaxResultLength() - readResult.getConsumedLength(), data);
                        } catch (CancellationException | InterruptedIOException ex) {
                            log("Read #%d (Offset=%d) has been interrupted.", readId, entry.getStreamSegmentOffset());
                            return;
                        } catch (Exception ex) {
                            log(ex, "Read #%d (Offset=%d)", readId, entry.getStreamSegmentOffset());
                        }
                    }

                    log("Read #%d completed with %d bytes read in total.", readId, readResult.getConsumedLength());
                }));
    }

    private void createTransaction(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String parentName = parsedCommand.getNext();
        checkArguments(parentName != null && parentName.length() > 0, Commands.combine(Commands.CREATE_TRANSACTION, Commands.PARENT_STREAM_SEGMENT_NAME));
        long startTime = getCurrentTime();
        await(this.streamSegmentStore.createTransaction(parentName, UUID.randomUUID(), defaultTimeout), r -> log(startTime, "Created Transaction %s with parent %s.", r, parentName));
    }

    private void mergeTransaction(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String transactionName = parsedCommand.getNext();
        checkArguments(transactionName != null && transactionName.length() > 0, Commands.combine(Commands.MERGE_TRANSACTION, Commands.TRANSACTION_STREAM_SEGMENT_NAME));
        long startTime = getCurrentTime();
        await(this.streamSegmentStore.mergeTransaction(transactionName, defaultTimeout), r -> log(startTime, "Merged Transaction %s into parent segment.", transactionName));
    }

    private <T> void await(CompletableFuture<T> future, Consumer<T> callback) {
        try {
            T result = future.join();
            CallbackHelpers.invokeSafely(callback, result, ex -> log(ex, "Callback failure"));
        } catch (CompletionException ex) {
            log(ex, "Unable to complete operation.");
        }
    }

    private void checkArguments(boolean isValid, String syntaxExample) throws InvalidCommandSyntax {
        if (!isValid) {
            throw new InvalidCommandSyntax(String.format("Invalid command syntax. Sample: %s", syntaxExample));
        }
    }

    private void log(String message, Object... args) {
        this.out.println(String.format(message, args));
    }

    private void log(long startTime, String message, Object... args) {
        long elapsedMillis = getElapsedMillis(startTime);
        this.out.println(String.format(message, args) + String.format(" (%dms)", elapsedMillis));
    }

    private void log(Throwable ex, String message, Object... args) {
        this.errorLogger.println(String.format("ERROR: %s", String.format(message, args)));
        ExceptionHelpers.getRealException(ex).printStackTrace(this.errorLogger);
    }

    private long getCurrentTime() {
        return System.nanoTime();
    }

    private long getElapsedMillis(long startTime) {
        return (System.nanoTime() - startTime) / 1000000;
    }

    //endregion

    //region Invalid Command Syntax

    private static class InvalidCommandSyntax extends RuntimeException {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        public InvalidCommandSyntax(String message) {
            super(message);
        }
    }

    //endregion

    //region Commands

    private static class Commands {
        public static final String CREATE = "create";
        public static final String DELETE = "delete";
        public static final String SEAL = "seal";
        public static final String GET = "get";
        public static final String APPEND = "append";
        public static final String READ = "read";
        public static final String CREATE_TRANSACTION = "transaction-create";
        public static final String MERGE_TRANSACTION = "transaction-merge";

        public static final AbstractMap<String, String> SYNTAXES = new TreeMap<>();

        static {
            SYNTAXES.put(CREATE, Commands.combine(CREATE, Commands.STREAM_SEGMENT_NAME));
            SYNTAXES.put(DELETE, Commands.combine(DELETE, Commands.STREAM_SEGMENT_NAME));
            SYNTAXES.put(APPEND, Commands.combine(APPEND, Commands.STREAM_SEGMENT_NAME, Commands.APPEND_DATA));
            SYNTAXES.put(READ, Commands.combine(READ, Commands.STREAM_SEGMENT_NAME));
            SYNTAXES.put(SEAL, Commands.combine(SEAL, Commands.STREAM_SEGMENT_NAME));
            SYNTAXES.put(GET, Commands.combine(GET, Commands.STREAM_SEGMENT_NAME, Commands.OFFSET, Commands.LENGTH));
            SYNTAXES.put(CREATE_TRANSACTION, Commands.combine(CREATE_TRANSACTION, Commands.PARENT_STREAM_SEGMENT_NAME));
            SYNTAXES.put(MERGE_TRANSACTION, Commands.combine(MERGE_TRANSACTION, Commands.TRANSACTION_STREAM_SEGMENT_NAME));
        }

        private static final String PARENT_STREAM_SEGMENT_NAME = "<parent-stream-segment-name>";
        private static final String TRANSACTION_STREAM_SEGMENT_NAME = "<transaction-stream-segment-name>";
        private static final String STREAM_SEGMENT_NAME = "<stream-segment-name>";
        private static final String APPEND_DATA = "<append-string>";
        private static final String OFFSET = "<offset>";
        private static final String LENGTH = "<length>";

        private static String combine(String commandName, String... args) {
            StringBuilder result = new StringBuilder();
            result.append(commandName);
            for (String arg : args) {
                result.append(" ");
                result.append(arg);
            }

            return result.toString();
        }
    }

    //endregion

    //region CommandLineParser

    private static class CommandLineParser {
        private static final char SPACE = ' ';
        private static final char QUOTE = '"';
        private final String input;
        private int pos;

        public CommandLineParser(String input) {
            this.input = input;
            this.pos = 0;
        }

        public String getNext() {

            //Skip over consecutive spaces.
            while (pos < this.input.length() && input.charAt(pos) == SPACE) {
                pos++;
            }

            if (pos >= this.input.length()) {
                return null;
            }

            //pos is at the start of a word
            int nextQuotePos = this.input.indexOf(QUOTE, pos);
            if (nextQuotePos == pos) {
                //we are sitting on a quote; find the next quote and then return the whole contents.
                pos = nextQuotePos + 1;
                return getResult(QUOTE);
            }

            //No quote; just find the next space.
            return getResult(SPACE);
        }

        public int getNextOrDefault(int defaultValue) {
            String next = getNext();
            if (next == null) {
                return defaultValue;
            }

            try {
                return Integer.parseInt(next);
            } catch (NumberFormatException ex) {
                return defaultValue;
            }
        }

        private String getResult(char delimiter) {
            int endPos = this.input.indexOf(delimiter, pos);
            if (endPos < 0) {
                // no ending quote; read till end of stream
                endPos = this.input.length();
            }

            String result = this.input.substring(pos, endPos);
            pos = endPos + 1;
            return result;
        }
    }

    //endregion
}
