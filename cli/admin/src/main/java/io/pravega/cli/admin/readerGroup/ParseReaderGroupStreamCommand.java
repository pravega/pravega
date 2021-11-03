package io.pravega.cli.admin.readerGroup;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ParseReaderGroupStreamCommand extends AdminCommand {

    protected final GrpcAuthHelper authHelper;
    private static final int REQUEST_TIMEOUT_SECONDS = 10;
    private static final int READ_WRITE_BUFFER_SIZE = 2 * 1024 * 1024;

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
        String segment = NameUtils.getQualifiedStreamSegmentName(scope, stream, 0);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient);
        readRGSegmentToFile(segmentHelper, segmentStoreHost, segment, fileName);
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
        File file = createFileAndDirectory(fileName);

        @Cleanup
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        long bufferLength = READ_WRITE_BUFFER_SIZE;
        CompletableFuture<WireCommands.SegmentRead> reply = segmentHelper.readSegment(fullyQualifiedSegmentName,
                0, (int) bufferLength, new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), authHelper.retrieveMasterToken());
        WireCommands.SegmentRead bufferRead = reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        int bytesRead = bufferRead.getData().readableBytes();
        final byte[] bytes = new byte[bytesRead];
        bufferRead.getData().getBytes(bufferRead.getData().readerIndex(), bytes);
        ByteBuffer b = ByteBuffer.wrap(bytes);

        val serializer = new UpdateOrInitSerializer<>(new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(), new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer());
        val state = serializer.deserialize(b);
        writer.write(state.toString());
        writer.newLine();
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
