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
package io.pravega.cli.admin.storage;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.containers.DebugStorageSegment;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;

/**
 * Reads (and downloads) a range of bytes from a segment (it can be a main segment or attribute segment, as long as it's
 * a valid LTS segment).
 */
public class ReadSegmentCommand extends StorageCommand {
    private static final long REPORT_FREQUENCY = 10 * 1024 * 1024;

    public ReadSegmentCommand(CommandArgs args) {
        super(args);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "read-segment", "Reads the contents of a segment from Storage.",
                new ArgDescriptor("segment-name", "Fully qualified segment name (include scope and stream)"),
                new ArgDescriptor("offset", "Offset to start reading at"),
                new ArgDescriptor("length", "Number of bytes to read"),
                new ArgDescriptor("output-file-path", "Path to (local) file where to write the result"));
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(4);
        final String segmentName = getArg(0);
        final long offset = getLongArg(1);
        final int length = getIntArg(2);
        final String targetPath = getArg(3);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(segmentName), "Invalid segment name");
        val targetFile = Paths.get(targetPath).toFile();
        if (!maybeCreateFile(targetFile)) {
            return;
        }

        @Cleanup
        val storage = this.storageFactory.createStorageAdapter();
        storage.initialize(Integer.MAX_VALUE);
        @Cleanup
        val segment = new DebugStorageSegment(segmentName, storage, executorService());

        @Cleanup
        val readResult = segment.read(offset, length).get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        long bytesWritten = 0L;
        long lastReport = 0L;
        try (FileChannel channel = FileChannel.open(targetFile.toPath(), StandardOpenOption.WRITE)) {
            while (readResult.hasNext()) {
                val entry = readResult.next();
                entry.requestContent(DEFAULT_TIMEOUT);
                val content = entry.getContent().get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                val bufferIterator = content.iterateBuffers();
                while (bufferIterator.hasNext()) {
                    val buffer = bufferIterator.next();
                    while (buffer.hasRemaining()) {
                        int c = channel.write(buffer);
                        assert c > 0;
                        bytesWritten += c;
                        if (bytesWritten - lastReport >= REPORT_FREQUENCY) {
                            output("Read %s bytes into '%s'.", bytesWritten, targetFile);
                            lastReport = bytesWritten;
                        }
                    }
                }
            }
        }

        output("Read a total of %s bytes into '%s'.", bytesWritten, targetFile);
    }
}
