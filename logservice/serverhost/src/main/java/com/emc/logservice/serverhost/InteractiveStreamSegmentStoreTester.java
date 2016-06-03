package com.emc.logservice.serverhost;

import com.emc.logservice.common.CallbackHelpers;
import com.emc.logservice.common.StreamHelpers;
import com.emc.logservice.contracts.*;
import com.emc.logservice.server.*;
import com.emc.logservice.server.containers.StreamSegmentContainer;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;
import com.emc.logservice.server.logs.DurableLogFactory;
import com.emc.logservice.server.reading.ReadIndexFactory;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageabstraction.StorageFactory;

import java.io.*;
import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Interactive (command-line) StreamSegmentStore tester.
 */
public class InteractiveStreamSegmentStoreTester implements AutoCloseable {
    private final StreamSegmentContainer container;
    private final Duration DefaultTimeout = Duration.ofSeconds(30);
    private final PrintStream out;
    private final PrintStream errorLogger;
    private final BufferedReader in;
    private final UUID clientId = UUID.randomUUID();
    private int appendCount;
    private int readCount;

    public InteractiveStreamSegmentStoreTester(DurableDataLogFactory dataLogFactory, StorageFactory storageFactory, InputStream in, PrintStream out, PrintStream errorLogger) {
        MetadataRepository metadataRepository = new InMemoryMetadataRepository();
        OperationLogFactory durableLogFactory = new DurableLogFactory(dataLogFactory);
        CacheFactory cacheFactory = new ReadIndexFactory();
        this.out = out;
        this.in = new BufferedReader(new InputStreamReader(in));
        this.errorLogger = errorLogger;
        this.container = new StreamSegmentContainer("test", metadataRepository, durableLogFactory, cacheFactory, storageFactory);
        log("ClientId = %s", clientId);
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
            while (true) {
                //this.out.print(">"); Commenting out since this interferes with printing read output.
                CommandLineParser commandParser = new CommandLineParser(this.in.readLine());
                String commandName = commandParser.getNext();
                if (commandName == null) {
                    break;
                }

                try {
                    switch (commandName) {
                        case CommandTokens.Create:
                            createStream(commandParser);
                            break;
                        case CommandTokens.Delete:
                            deleteStream(commandParser);
                            break;
                        case CommandTokens.Seal:
                            sealStream(commandParser);
                            break;
                        case CommandTokens.Get:
                            getStreamInfo(commandParser);
                            break;
                        case CommandTokens.Append:
                            appendToStream(commandParser);
                            break;
                        case CommandTokens.Read:
                            readFromStream(commandParser);
                            break;
                        case CommandTokens.CreateBatch:
                            createBatch(commandParser);
                            break;
                        case CommandTokens.MergeBatch:
                            mergeBatch(commandParser);
                            break;
                        default:
                            log("Unknown command '%s'.", commandName);
                            break;
                    }
                }
                catch (InvalidCommandSyntax ex) {
                    log(ex.getMessage());
                }
            }
        }
        catch (IOException ex) {
            log(ex, "Could not read from input.");
        }
        finally {
            // Stop container upon exit
            this.container.stop(DefaultTimeout).join();
            log("Container '%s' stopped.", this.container.getId());
        }
    }

    private void createStream(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, CommandTokens.combine(CommandTokens.Create, CommandTokens.StreamSegmentName));
        await(this.container.createStreamSegment(name, DefaultTimeout), r -> log("Created StreamSegment %s.", name));
    }

    private void deleteStream(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, CommandTokens.combine(CommandTokens.Delete, CommandTokens.StreamSegmentName));
        await(this.container.deleteStreamSegment(name, DefaultTimeout), r -> log("Deleted StreamSegment %s.", name));
    }

    private void sealStream(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, CommandTokens.combine(CommandTokens.Get, CommandTokens.StreamSegmentName));
        await(this.container.sealStreamSegment(name, DefaultTimeout), r -> log("Sealed StreamSegment %s.", name));
    }

    private void getStreamInfo(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, CommandTokens.combine(CommandTokens.Get, CommandTokens.StreamSegmentName));
        await(this.container.getStreamSegmentInfo(name, DefaultTimeout), result ->
                log("Name = %s, Length = %d, Sealed = %s, Deleted = %s.", result.getName(), result.getLength(), result.isSealed(), result.isDeleted()));
    }

    private void appendToStream(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String name = parsedCommand.getNext();
        String data = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0 && data != null && data.length() > 0,
                CommandTokens.combine(CommandTokens.Append, CommandTokens.StreamSegmentName, CommandTokens.AppendData));
        appendToStream(name, data.getBytes());
    }

    private void appendToStream(String name, byte[] data) throws InvalidCommandSyntax {
        AppendContext context = new AppendContext(clientId, appendCount++);
        await(this.container.append(name, data, context, DefaultTimeout), r -> log("Appended %d bytes at offset %d.", data.length, r));
    }

    private void readFromStream(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        // read name offset length
        String name = parsedCommand.getNext();
        int offset = parsedCommand.getNextOrDefault(Integer.MIN_VALUE);
        int length = parsedCommand.getNextOrDefault(Integer.MIN_VALUE);

        checkArguments(name != null && name.length() > 0 && offset >= 0 && length > 0,
                CommandTokens.combine(CommandTokens.Read, CommandTokens.StreamSegmentName, CommandTokens.Offset, CommandTokens.Length));

        final int readId = this.readCount++;
        log("Started Read #%d from %s.", readId, name);
        await(this.container.read(name, offset, length, DefaultTimeout), readResult ->
                CompletableFuture.runAsync(() -> {
                    while (readResult.hasNext()) {
                        ReadResultEntry entry = readResult.next();
                        try {
                            ReadResultEntryContents contents = entry.getContent().get();
                            byte[] rawData = new byte[contents.getLength()];
                            StreamHelpers.readAll(contents.getData(), rawData, 0, rawData.length);
                            String data = new String(rawData);
                            log("Read #%d (Offset=%d, Remaining=%d): %s", readId, entry.getStreamSegmentOffset(), readResult.getMaxResultLength() - readResult.getConsumedLength(), data);
                        }
                        catch (InterruptedIOException ex) {
                            log("Read #%d (Offset=%d) has been interrupted.", readId, entry.getStreamSegmentOffset());
                        }
                        catch (Exception ex) {
                            log(ex, "Read #%d (Offset=%d)", readId, entry.getStreamSegmentOffset());
                        }
                    }
                }));
    }

    private void createBatch(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String parentName = parsedCommand.getNext();
        checkArguments(parentName != null && parentName.length() > 0, CommandTokens.combine(CommandTokens.CreateBatch, CommandTokens.ParentStreamSegmentName));
        await(this.container.createBatch(parentName, DefaultTimeout), r -> log("Created BatchStreamSegment %s with parent %s.", r, parentName));
    }

    private void mergeBatch(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String batchStreamName = parsedCommand.getNext();
        checkArguments(batchStreamName != null && batchStreamName.length() > 0, CommandTokens.combine(CommandTokens.MergeBatch, CommandTokens.BatchStreamSegmentName));
        await(this.container.mergeBatch(batchStreamName, DefaultTimeout), r -> log("Merged BatchStreamSegment %s into parent stream.", batchStreamName));

    }

    private <T> void await(CompletableFuture<T> future, Consumer<T> callback) {
        try {
            T result = future.join();
            CallbackHelpers.invokeSafely(callback, result, ex -> log(ex, "Callback failure"));
        }
        catch (CompletionException ex) {
            log(ex, "Unable to complete operation.");
        }
    }

    @Override
    public void close() {
        try {
            this.container.close();
            log("Container '%s' closed.", this.container.getId());
        }
        catch (Exception ex) {
            log(ex, "Unable to close container '%s'.", this.container.getId());
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

    private void log(Throwable ex, String message, Object... args) {
        this.errorLogger.println(String.format("ERROR: %s", String.format(message, args)));
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

    private static class InvalidCommandSyntax extends Exception {
        public InvalidCommandSyntax(String message) {
            super(message);
        }
    }

    private static class CommandTokens {
        public static final String Create = "create";
        public static final String Delete = "delete";
        public static final String Seal = "seal";
        public static final String Get = "get";
        public static final String Append = "append";
        public static final String Read = "read";
        public static final String CreateBatch = "batch-create";
        public static final String MergeBatch = "batch-merge";

        public static final String ParentStreamSegmentName = "<parent-stream-segment-name>";
        public static final String BatchStreamSegmentName = "<batch-stream-segment-name>";
        public static final String StreamSegmentName = "<stream-segment-name>";
        public static final String AppendData = "<append-string>";
        public static final String Offset = "<offset>";
        public static final String Length = "<length>";

        public static String combine(String commandName, String... args) {
            StringBuilder result = new StringBuilder();
            result.append(commandName);
            for (String arg : args) {
                result.append(" ");
                result.append(arg);
            }

            return result.toString();
        }
    }

    private static class CommandLineParser {
        private static char Space = ' ';
        private static char Quote = '"';
        private final String input;
        private int pos;

        public CommandLineParser(String input) {
            this.input = input;
            this.pos = 0;
        }

        public String getNext() {

            //Skip over consecutive spaces.
            while (pos < this.input.length() && input.charAt(pos) == Space) {
                pos++;
            }

            if (pos >= this.input.length()) {
                return null;
            }

            //pos is at the start of a word
            int nextQuotePos = this.input.indexOf(Quote, pos);
            if (nextQuotePos == pos) {
                //we are sitting on a quote; find the next quote and then return the whole contents.
                pos = nextQuotePos + 1;
                return getResult(Quote);
            }

            //No quote; just find the next space.
            return getResult(Space);
        }

        public int getNextOrDefault(int defaultValue) {
            String next = getNext();
            if (next == null) {
                return defaultValue;
            }

            try {
                return Integer.parseInt(next);
            }
            catch (NumberFormatException ex) {
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
}
