package com.emc.logservice.serverhost;

import com.emc.logservice.common.CallbackHelpers;
import com.emc.logservice.common.StreamHelpers;
import com.emc.logservice.contracts.*;
import com.emc.logservice.server.service.ServiceBuilder;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Interactive (command-line) StreamSegmentStore tester.
 */
public class InteractiveStreamSegmentStoreTester {
    //region Members

    private final StreamSegmentStore streamSegmentStore;
    private final Duration DefaultTimeout = Duration.ofSeconds(30);
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

    //region Command Execution

    public void run() {
        log("InteractiveStreamStoreTester: ClientId = %s.", clientId);
        log("Available commands:");
        for (String syntax : Commands.Syntaxes.values()) {
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
                        case Commands.Create:
                            createStream(commandParser);
                            break;
                        case Commands.Delete:
                            deleteStream(commandParser);
                            break;
                        case Commands.Seal:
                            sealStream(commandParser);
                            break;
                        case Commands.Get:
                            getStreamInfo(commandParser);
                            break;
                        case Commands.Append:
                            appendToStream(commandParser);
                            break;
                        case Commands.Read:
                            readFromStream(commandParser);
                            break;
                        case Commands.CreateBatch:
                            createBatch(commandParser);
                            break;
                        case Commands.MergeBatch:
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
    }

    private void createStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.Syntaxes.get(Commands.Create));
        await(this.streamSegmentStore.createStreamSegment(name, DefaultTimeout), r -> log("Created StreamSegment %s.", name));
    }

    private void deleteStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.Syntaxes.get(Commands.Delete));
        await(this.streamSegmentStore.deleteStreamSegment(name, DefaultTimeout), r -> log("Deleted StreamSegment %s.", name));
    }

    private void sealStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.Syntaxes.get(Commands.Get));
        await(this.streamSegmentStore.sealStreamSegment(name, DefaultTimeout), r -> log("Sealed StreamSegment %s.", name));
    }

    private void getStreamInfo(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0, Commands.Syntaxes.get(Commands.Get));
        await(this.streamSegmentStore.getStreamSegmentInfo(name, DefaultTimeout), result ->
                log("Name = %s, Length = %d, Sealed = %s, Deleted = %s.", result.getName(), result.getLength(), result.isSealed(), result.isDeleted()));
    }

    private void appendToStream(CommandLineParser parsedCommand) {
        String name = parsedCommand.getNext();
        String data = parsedCommand.getNext();
        checkArguments(name != null && name.length() > 0 && data != null && data.length() > 0,
                Commands.Syntaxes.get(Commands.Append));
        appendToStream(name, data.getBytes());
    }

    private void appendToStream(String name, byte[] data) {
        AppendContext context = new AppendContext(clientId, appendCount++);
        await(this.streamSegmentStore.append(name, data, context, DefaultTimeout), r -> log("Appended %d bytes at offset %d.", data.length, r));
    }

    private void readFromStream(CommandLineParser parsedCommand) {
        // read name offset length
        String name = parsedCommand.getNext();
        int offset = parsedCommand.getNextOrDefault(Integer.MIN_VALUE);
        int length = parsedCommand.getNextOrDefault(Integer.MIN_VALUE);

        checkArguments(name != null && name.length() > 0 && offset >= 0 && length > 0, Commands.Syntaxes.get(Commands.Read));

        final int readId = this.readCount++;
        log("Started Read #%d from %s.", readId, name);
        await(this.streamSegmentStore.read(name, offset, length, DefaultTimeout), readResult ->
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
                        catch (CancellationException | InterruptedIOException ex) {
                            log("Read #%d (Offset=%d) has been interrupted.", readId, entry.getStreamSegmentOffset());
                            return;
                        }
                        catch (Exception ex) {
                            log(ex, "Read #%d (Offset=%d)", readId, entry.getStreamSegmentOffset());
                        }
                    }

                    log("Read #%d (Offset=%d) completed with %d bytes read in total.", readId, readResult.getConsumedLength());
                }));
    }

    private void createBatch(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String parentName = parsedCommand.getNext();
        checkArguments(parentName != null && parentName.length() > 0, Commands.combine(Commands.CreateBatch, Commands.ParentStreamSegmentName));
        await(this.streamSegmentStore.createBatch(parentName, DefaultTimeout), r -> log("Created BatchStreamSegment %s with parent %s.", r, parentName));
    }

    private void mergeBatch(CommandLineParser parsedCommand) throws InvalidCommandSyntax {
        String batchStreamName = parsedCommand.getNext();
        checkArguments(batchStreamName != null && batchStreamName.length() > 0, Commands.combine(Commands.MergeBatch, Commands.BatchStreamSegmentName));
        await(this.streamSegmentStore.mergeBatch(batchStreamName, DefaultTimeout), r -> log("Merged BatchStreamSegment %s into parent stream.", batchStreamName));
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

    //endregion

    //region Invalid Command Syntax

    private static class InvalidCommandSyntax extends RuntimeException {
        public InvalidCommandSyntax(String message) {
            super(message);
        }
    }

    //endregion

    //region Commands

    private static class Commands {
        public static final String Create = "create";
        public static final String Delete = "delete";
        public static final String Seal = "seal";
        public static final String Get = "get";
        public static final String Append = "append";
        public static final String Read = "read";
        public static final String CreateBatch = "batch-create";
        public static final String MergeBatch = "batch-merge";

        public static final AbstractMap<String, String> Syntaxes = new TreeMap<>();

        static {
            Syntaxes.put(Create, Commands.combine(Create, Commands.StreamSegmentName));
            Syntaxes.put(Delete, Commands.combine(Delete, Commands.StreamSegmentName));
            Syntaxes.put(Seal, Commands.combine(Seal, Commands.StreamSegmentName));
            Syntaxes.put(Get, Commands.combine(Get, Commands.StreamSegmentName, Commands.Offset, Commands.Length));
            Syntaxes.put(Append, Commands.combine(Append, Commands.BatchStreamSegmentName, Commands.AppendData));
            Syntaxes.put(Read, Commands.combine(Read, Commands.BatchStreamSegmentName));
            Syntaxes.put(CreateBatch, Commands.combine(CreateBatch, Commands.ParentStreamSegmentName));
            Syntaxes.put(MergeBatch, Commands.combine(MergeBatch, Commands.BatchStreamSegmentName));
        }

        private static final String ParentStreamSegmentName = "<parent-stream-segment-name>";
        private static final String BatchStreamSegmentName = "<batch-stream-segment-name>";
        private static final String StreamSegmentName = "<stream-segment-name>";
        private static final String AppendData = "<append-string>";
        private static final String Offset = "<offset>";
        private static final String Length = "<length>";

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

    //endregion
}
