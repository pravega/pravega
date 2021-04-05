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

import com.google.common.base.Charsets;
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.containers.DebugStorageSegment;
import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Base class for any {@link AdminCommand} that deals directly with LTS Storage.
 */
public abstract class StorageCommand extends AdminCommand {
    protected final static String COMPONENT = "storage";
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    protected final StorageFactory storageFactory;

    /**
     * Creates a new instance of the DataRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    StorageCommand(CommandArgs args) {
        super(args);
        this.storageFactory = createStorageFactory(executorService());
    }

    protected ScheduledExecutorService executorService() {
        return getCommandArgs().getState().getExecutor();
    }

    /**
     * Creates the {@link StorageFactory} instance by reading the config values.
     *
     * @param executorService A thread pool for execution.
     * @return A newly created {@link StorageFactory} instance.
     */
    private StorageFactory createStorageFactory(ScheduledExecutorService executorService) {
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getCommandArgs().getState().getConfigBuilder().build());
        StorageLoader loader = new StorageLoader();
        return loader.load(configSetupHelper, getServiceConfig().getStorageImplementation().toString(),
                getServiceConfig().getStorageLayout(), executorService);
    }

    protected boolean maybeCreateFile(File file) throws Exception {
        if (file.exists()) {
            output("Target path '%s' exists. Proceeding with the command will OVERWRITE it.", file);
            if (!confirmContinue()) {
                return false;
            }
        }

        if (file.exists()) {
            file.delete();
            output("Deleted existing file '%s'.", file);
        }

        file.createNewFile();
        output("Created empty file '%s'.", file);
        return true;
    }

    protected String formatAttribute(Map.Entry<UUID, Long> attribute) {
        return String.format("%s: %s", attribute.getKey(), attribute.getValue());
    }

    protected String formatTableEntry(DebugStorageSegment.TableEntryInfo e) {
        if (e == null) {
            return "(null)";
        }

        if (e instanceof DebugStorageSegment.ValidTableEntryInfo) {
            val ve = (DebugStorageSegment.ValidTableEntryInfo) e;
            String r = String.format("Bucket=[%s, %s], EntryOffset=%s, Key=%s, ",
                    e.getBucketHash(), e.getBucketOffset(), ve.getEntryOffset(), formatKey(ve.getEntry().getKey()));
            if (ve.isDeleted()) {
                r += "DELETED";
            } else {
                r += String.format("Value=(%s bytes)", ve.getEntry().getValue().getLength());
            }
            if (ve.getExtraInfo() != null) {
                r += ", " + ve.getExtraInfo();
            }
            return r;
        } else if (e instanceof DebugStorageSegment.InvalidTableEntryInfo) {
            val ie = (DebugStorageSegment.InvalidTableEntryInfo) e;
            return String.format("Bucket=[%s, %s], EntryOffset=%s, Exception=%s",
                    e.getBucketHash(), e.getBucketOffset(), ie.getEntryOffset(),
                    ie.getException());
        }
        return e.toString();
    }

    private String formatKey(TableKey key) {
        return String.format("[%s, %s]", key.getVersion(), new String(key.getKey().getCopy(), Charsets.UTF_8));
    }

    //region ResultWriter and implementations

    protected static abstract class ResultWriter implements AutoCloseable {
        @Override
        public void close() {
        }

        boolean initialize() {
            return true;
        }

        abstract boolean write(String data);
    }

    @RequiredArgsConstructor
    protected class ConsoleWriter extends ResultWriter {
        private final int maxAtOnce;
        private int countSinceLastConfirm = 0;

        @Override
        boolean write(String data) {
            output("\t%s", data);
            if (++this.countSinceLastConfirm >= this.maxAtOnce) {
                if (!confirmContinue()) {
                    return false;
                }
                this.countSinceLastConfirm = 0;
            }
            return true;
        }
    }

    @RequiredArgsConstructor
    protected class FileWriter extends ResultWriter {
        private final Path path;
        private FileChannel channel;

        @SneakyThrows(Exception.class)
        boolean initialize() {
            val targetFile = this.path.toFile();
            if (!maybeCreateFile(targetFile)) {
                return false;
            }
            this.channel = FileChannel.open(this.path, StandardOpenOption.WRITE);
            return true;
        }

        @Override
        @SneakyThrows(IOException.class)
        public void close() {
            val c = this.channel;
            if (c != null) {
                c.close();
            }
        }

        @Override
        @SneakyThrows(IOException.class)
        boolean write(String data) {
            if (!data.endsWith(System.lineSeparator())) {
                data = data + System.lineSeparator();
            }
            this.channel.write(ByteBuffer.wrap(data.getBytes(Charsets.UTF_8)));
            return true;
        }
    }

    //endregion
}
