/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.debug;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.state.impl.UpdateOrInitSerializer;
import lombok.Cleanup;
import lombok.val;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * The command takes input stream file and output stream file as argument.
 * It parses the serialized ReaderGroup Stream file to a human readable file.
 */
public class ReaderGroupStreamFileParsingCommand extends AdminCommand {
    private final static int HEADER = 4;
    private final static int LENGTH = 4;
    private final static int TYPE = 0;

    public ReaderGroupStreamFileParsingCommand(CommandArgs args) {
        super(args);
    }
    
    @Override
    public void execute() {
        try {
            ensureArgCount(2);
            String inputFileName = getInputStreamFilename(getCommandArgs().getArgs());
            String outputFileName = getOutputStreamFilename(getCommandArgs().getArgs());

            @Cleanup
            FileInputStream fileInputStream = new FileInputStream(new File(inputFileName));
            @Cleanup
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));

            while (fileInputStream.available() > 0) {
                // read type
                // type should be 0 as Wirecommand.Event type is 0
                byte[] type = new byte[HEADER];
                int read = fileInputStream.read(type);
                assertEquals(read, HEADER);
                ByteBuffer b = ByteBuffer.wrap(type);
                int t = b.getInt();
                assertEquals(t, TYPE);

                // read length
                byte[] len = new byte[LENGTH];
                read = fileInputStream.read(len);
                assertEquals(read, LENGTH);
                b = ByteBuffer.wrap(len);
                int eventLength = b.getInt();

                byte[] payload = new byte[eventLength];
                read = fileInputStream.read(payload);
                assertEquals(read, eventLength);
                b = ByteBuffer.wrap(payload);

                val serializer = new UpdateOrInitSerializer<>(new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(), new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer());
                val state = serializer.deserialize(b);
                writer.write(state.toString());
                writer.newLine();
            }
        } catch (RuntimeException e) {
            System.err.println("Runtime exception " + e.getMessage());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private String getInputStreamFilename(List<String> userInput) {
        return userInput.get(0);
    }

    private String getOutputStreamFilename(List<String> userInput) {
        return userInput.get(1);
    }

    public static CommandDescriptor descriptor() {
        final String component = "debug";
        return new CommandDescriptor(component, "parse-rg-stream-file", "Parse ReaderGroup stream file to" +
                " human readable for debugging purpose", new ArgDescriptor("inputFile",
                "Path of the ReaderGroup stream file"), new ArgDescriptor("outputFile",
                "Output file of the deserialized ReaderGroup stream file"));
    }
}

