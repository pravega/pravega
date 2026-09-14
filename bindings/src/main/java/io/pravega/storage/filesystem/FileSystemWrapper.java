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
package io.pravega.storage.filesystem;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper for File System calls.
 */
@Slf4j
public class FileSystemWrapper implements AutoCloseable {
    /**
     * Set of PosixFilePermission for readonly file.
     */
    static final Set<PosixFilePermission> READ_ONLY_PERMISSION = ImmutableSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.OTHERS_READ);

    /**
     * Set of PosixFilePermission for writable file.
     */
    static final Set<PosixFilePermission> READ_WRITE_PERMISSION = ImmutableSet.of(
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.OTHERS_READ);

    private static final long MAX_CLEANUP_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private final Cache<Path, SharedFileChannel> readCache;
    private final Cache<Path, SharedFileChannel> writeCache;
    private final ScheduledExecutorService cleanupExecutor;
    private final ScheduledFuture<?> cleanupTask;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a wrapper with bounded read and write channel caches.
     *
     * @param readCacheSize Maximum number of cached read channels.
     * @param writeCacheSize Maximum number of cached write channels.
     * @param cacheExpiration Amount of inactivity after which a channel is evicted.
     */
    public FileSystemWrapper(int readCacheSize, int writeCacheSize, Duration cacheExpiration) {
        this(readCacheSize, writeCacheSize, cacheExpiration, Ticker.systemTicker());
    }

    FileSystemWrapper(int readCacheSize, int writeCacheSize, Duration cacheExpiration, Ticker ticker) {
        this.readCache = buildCache(readCacheSize, cacheExpiration, ticker);
        this.writeCache = buildCache(writeCacheSize, cacheExpiration, ticker);
        this.cleanupExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "filesystem-channel-cache");
        long cleanupIntervalMillis = Math.max(1,
                Math.min(cacheExpiration.toMillis(), MAX_CLEANUP_INTERVAL_MILLIS));
        this.cleanupTask = cleanupExecutor.scheduleWithFixedDelay(this::cleanUpCaches, cleanupIntervalMillis,
                cleanupIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private Cache<Path, SharedFileChannel> buildCache(int maximumSize, Duration expiration, Ticker ticker) {
        RemovalListener<Path, SharedFileChannel> removalListener = this::handleRemovalNotification;
        return CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterAccess(expiration.toMillis(), TimeUnit.MILLISECONDS)
                .ticker(ticker)
                .removalListener(removalListener)
                .build();
    }

    private void handleRemovalNotification(RemovalNotification<Path, SharedFileChannel> notification) {
        SharedFileChannel channel = notification.getValue();
        if (channel != null) {
            channel.retire();
        }
    }

    /**
     * Creates a file by calling {@link Files#createFile(Path, FileAttribute[])} .
     * @param fileAttributes File attributes.
     * @param path Path for the file to create.
     * @return Path of created file.
     * @throws IOException Exception thrown by file system call.
     */
    Path createFile(FileAttribute<Set<PosixFilePermission>> fileAttributes, Path path) throws IOException {
        invalidate(path);
        return Files.createFile(path, fileAttributes);
    }

    /**
     * Create a directories for the given path by calling {@link Files#createDirectories(Path, FileAttribute[])}.
     * @param parent Path to the parent.
     * @return Path of created directory.
     * @throws IOException Exception thrown by file system call.
     */
    Path createDirectories(Path parent) throws IOException {
        return Files.createDirectories(parent);
    }

    /**
     * Deletes given path by calling {@link Files#delete(Path)}.
     * @param path File/directory to delete.
     * @throws IOException Exception thrown by file system call.
     */
    void delete(Path path) throws IOException {
        invalidate(path);
        Files.delete(path);
    }

    /**
     * Checks whether given file is a regular file
     * by calling {@link Files#isRegularFile(Path, LinkOption...)} and {@link Files#isDirectory(Path, LinkOption...)}.
     * @param path File path.
     * @return
     */
    boolean isRegularFile(Path path) {
        return Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                && !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
    }

    /**
     * Checks whether given file is writable by calling {@link Files#isWritable(Path)}.
     * @param path File path.
     * @return rue if file is writable, false otherwise.
     */
    boolean isWritable(Path path) {
        return Files.isWritable(path);
    }

    /**
     * Checks whether given file exists by calling {@link Files#exists(Path, LinkOption...)}.
     * @param path File path.
     * @return True if file exists, false otherwise.
     */
    boolean exists(Path path) {
        return Files.exists(path);
    }

    /**
     * Acquires a cached channel for reading. The returned lease must be closed after the operation completes.
     *
     * @param path File path.
     * @return A lease for a readable channel.
     * @throws IOException If the channel cannot be opened or this wrapper is closed.
     */
    FileChannelLease getReadChannel(Path path) throws IOException {
        return getChannel(readCache, path, StandardOpenOption.READ);
    }

    /**
     * Acquires a cached channel for writing. The returned lease must be closed after the operation completes.
     *
     * @param path File path.
     * @return A lease for a writable channel.
     * @throws IOException If the channel cannot be opened or this wrapper is closed.
     */
    FileChannelLease getWriteChannel(Path path) throws IOException {
        return getChannel(writeCache, path, StandardOpenOption.WRITE);
    }

    /**
     * Gets the size of file in bytes.
     * @param path File path.
     * @return Size of the file.
     * @throws IOException Exception thrown by file system call.
     */
    long  getFileSize(Path path) throws IOException {
        return Files.size(path);
    }

    /**
     * Gets the used space in bytes corresponding to the partition/volume for given path.
     * @param path File path.
     * @return Used space in bytes.
     */
    long getUsedSpace(Path path) {
        File file = path.toFile();
        return file.getTotalSpace() - file.getUsableSpace();
    }

    /**
     * Sets a file's POSIX permissions by calling {@link Files#setPosixFilePermissions(Path, Set)}.
     * @param path Path to the file.
     * @param permissions The new set of permissions.
     * @return Path
     * @throws IOException
     */
    Path setPermissions(Path path, Set<PosixFilePermission>  permissions) throws IOException {
        invalidate(path);
        return Files.setPosixFilePermissions(path, permissions);
    }

    private void invalidate(Path path) {
        Path cacheKey = getCacheKey(path);
        readCache.invalidate(cacheKey);
        writeCache.invalidate(cacheKey);
    }

    private void cleanUpCaches() {
        readCache.cleanUp();
        writeCache.cleanUp();
    }

    /**
     * Stops cache maintenance, prevents new leases, and retires all cached channels. Channels with active leases are
     * closed when their final lease is released; all other channels are closed before this method returns.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cleanupTask.cancel(false);
            ExecutorServiceHelpers.shutdown(cleanupExecutor);
            readCache.invalidateAll();
            writeCache.invalidateAll();
            cleanUpCaches();
        }
    }

    private FileChannelLease getChannel(Cache<Path, SharedFileChannel> cache, Path path,
                                        StandardOpenOption openOption) throws IOException {
        Path cacheKey = getCacheKey(path);
        while (!closed.get()) {
            SharedFileChannel sharedChannel = loadChannel(cache, cacheKey, openOption);
            FileChannelLease lease = sharedChannel.acquire();
            if (lease != null) {
                if (!closed.get()) {
                    return lease;
                }

                try (FileChannelLease ignored = lease) {
                    retireCachedChannel(cache, cacheKey, sharedChannel);
                }
            } else {
                retireCachedChannel(cache, cacheKey, sharedChannel);
            }
        }

        throw new IOException("FileSystemWrapper is closed.");
    }

    private void retireCachedChannel(Cache<Path, SharedFileChannel> cache, Path cacheKey,
                                     SharedFileChannel sharedChannel) {
        if (!cache.asMap().remove(cacheKey, sharedChannel)) {
            sharedChannel.retire();
        }
    }

    private SharedFileChannel loadChannel(Cache<Path, SharedFileChannel> cache, Path path,
                                          StandardOpenOption openOption) throws IOException {
        try {
            return cache.get(path, () -> new SharedFileChannel(path, FileChannel.open(path, openOption)));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Unable to open file channel for " + path, e.getCause());
        }
    }

    private Path getCacheKey(Path path) {
        return path.toAbsolutePath().normalize();
    }

    /**
     * A local ownership token for a cached channel. Closing a lease releases the channel back to its cache; it does not
     * close a channel that is still cached or leased by another operation.
     */
    interface FileChannelLease extends AutoCloseable {
        FileChannel getChannel();

        @Override
        void close();
    }

    private static final class FileChannelLeaseImpl implements FileChannelLease {
        private final SharedFileChannel owner;
        private final FileChannel channel;
        private final AtomicBoolean released = new AtomicBoolean();

        private FileChannelLeaseImpl(SharedFileChannel owner, FileChannel channel) {
            this.owner = owner;
            this.channel = channel;
        }

        @Override
        public FileChannel getChannel() {
            if (released.get()) {
                throw new IllegalStateException("File channel lease has already been released.");
            }
            return channel;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                owner.release();
            }
        }
    }

    private static final class SharedFileChannel {
        private final Path path;
        private final FileChannel channel;
        private int leaseCount;
        private boolean retired;

        private SharedFileChannel(Path path, FileChannel channel) {
            this.path = path;
            this.channel = channel;
        }

        synchronized FileChannelLease acquire() {
            if (retired || !channel.isOpen()) {
                return null;
            }

            leaseCount++;
            return new FileChannelLeaseImpl(this, channel);
        }

        synchronized void release() {
            if (leaseCount <= 0) {
                throw new IllegalStateException("File channel lease released more than once.");
            }

            leaseCount--;
            closeIfUnused();
        }

        synchronized void retire() {
            retired = true;
            closeIfUnused();
        }

        private void closeIfUnused() {
            if (retired && leaseCount == 0 && channel.isOpen()) {
                try {
                    channel.close();
                    log.debug("Closed cached channel for file {}.", path);
                } catch (IOException e) {
                    log.warn("Unable to close cached channel for file {}.", path, e);
                }
            }
        }
    }
}
