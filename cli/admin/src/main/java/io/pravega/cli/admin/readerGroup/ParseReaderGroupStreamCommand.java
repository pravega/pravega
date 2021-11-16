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
package io.pravega.cli.admin.readerGroup;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.segmentstore.ReadSegmentRangeCommand;
import io.pravega.cli.admin.utils.ConfigUtils;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.state.impl.UpdateOrInitSerializer;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ParseReaderGroupStreamCommand extends AdminCommand {

    private final static int HEADER = 4;
    private final static int LENGTH = 4;
    private final static int TYPE = 0;

    private static final int REQUEST_TIMEOUT_SECONDS = 10;
    private final GrpcAuthHelper authHelper;

    public ParseReaderGroupStreamCommand(CommandArgs args) {
        super(args);

        authHelper = new GrpcAuthHelper(true,
                "secret",
                600);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(4);
        final String scope = getArg(0);
        final String readerGroup = getArg(1);
        final String segmentStoreHost = getArg(2);
        final String fileName = getArg(3);
        String stream = NameUtils.getStreamForReaderGroup(readerGroup);
        String fullyQualifiedSegmentName = NameUtils.getQualifiedStreamSegmentName(scope, stream, 0);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient);
        readRGSegmentToFile(segmentHelper, segmentStoreHost, fullyQualifiedSegmentName, fileName);
    }

    /**
     * Reads the contents of the segment starting from the given offset and writes into the provided file.
     *
     * @param segmentHelper             A {@link SegmentHelper} instance to read the segment.
     * @param segmentStoreHost          Address of the segment-store to read from.
     * @param fullyQualifiedSegmentName The name of the segment.
     * @param fileName                  A name of the file to which the data will be written.
     * @throws IOException if the file create/write fails.
     * @throws Exception if the request fails.
     */
    private void readRGSegmentToFile(SegmentHelper segmentHelper, String segmentStoreHost, String fullyQualifiedSegmentName,
                                     String fileName) throws IOException, Exception {
        String tempfilename = "output/temp";
        File outputfile = createFileAndDirectory(fileName);

        @Cleanup
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputfile));

        CompletableFuture<WireCommands.StreamSegmentInfo> segmentInfo = segmentHelper.getSegmentInfo(fullyQualifiedSegmentName,
                            new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), authHelper.retrieveMasterToken(), 0L);
        WireCommands.StreamSegmentInfo streamSegmentInfo = segmentInfo.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        long startOffset = streamSegmentInfo.getStartOffset();
        long length = streamSegmentInfo.getWriteOffset();

        // create a temp file and write contents of the segment into it
        AdminCommandState state = new AdminCommandState();
        ConfigUtils.loadProperties(state);
        List<String> args = Arrays.asList(fullyQualifiedSegmentName, String.valueOf(startOffset), String.valueOf(length), segmentStoreHost, tempfilename);
        CommandArgs cmd = new CommandArgs(args, state);
        ReadSegmentRangeCommand rs = new ReadSegmentRangeCommand(cmd);
        rs.execute();

        try {
            FileInputStream fileInputStream = new FileInputStream(tempfilename);
            long offset = startOffset;
            while (fileInputStream.available() > 0) {
                // read type
                // type should be 0 as Wirecommand.Event type is 0
                byte[] type = new byte[HEADER];
                int read = fileInputStream.read(type);
                assertEquals("should read 4 bytes header", read, HEADER);
                ByteBuffer b = ByteBuffer.wrap(type);
                int t = b.getInt();
                assertEquals("Wirecommand.Event type should be 0", t, TYPE);

                // read length
                byte[] len = new byte[LENGTH];
                read = fileInputStream.read(len);
                assertEquals("read payload length", read, LENGTH);
                b = ByteBuffer.wrap(len);
                int eventLength = b.getInt();

                byte[] payload = new byte[eventLength];
                read = fileInputStream.read(payload);
                assertEquals("read payload", read, eventLength);
                b = ByteBuffer.wrap(payload);

                val serializer = new UpdateOrInitSerializer<>(new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(), new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer());
                val result = serializer.deserialize(b);
                writer.write("Offset: " + offset + "; State: " + result);
                writer.newLine();

                offset = offset + HEADER + LENGTH + eventLength;
            }
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        final String component = "readerGroup";
        return new CommandDescriptor(component, "parse-rg-stream", "Parse ReaderGroup Stream into a file",
                new ArgDescriptor("scope", "Name of the Scope"),
                new ArgDescriptor("reader-group-name", "Name of the Reader Group whose stream we want to parse"),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."),
                new ArgDescriptor("file-name", "Name of the file to write the contents into."));
    }
}
