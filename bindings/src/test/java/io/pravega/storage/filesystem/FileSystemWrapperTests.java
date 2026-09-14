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
import io.pravega.test.common.AssertExtensions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FileSystemWrapperTests {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testChannelsExpireAfterConfiguredInactivity() throws Exception {
        Path path = temporaryFolder.newFile().toPath();
        TestTicker ticker = new TestTicker();
        FileChannel secondRead;
        FileChannel secondWrite;

        try (FileSystemWrapper wrapper = new FileSystemWrapper(10, 10, Duration.ofMillis(10), ticker)) {
            FileChannel firstRead;
            FileChannel firstWrite;
            try (FileSystemWrapper.FileChannelLease readLease = wrapper.getReadChannel(path);
                 FileSystemWrapper.FileChannelLease repeatedReadLease = wrapper.getReadChannel(path);
                 FileSystemWrapper.FileChannelLease writeLease = wrapper.getWriteChannel(path);
                 FileSystemWrapper.FileChannelLease repeatedWriteLease = wrapper.getWriteChannel(path)) {
                firstRead = readLease.getChannel();
                firstWrite = writeLease.getChannel();
                assertSame(firstRead, repeatedReadLease.getChannel());
                assertSame(firstWrite, repeatedWriteLease.getChannel());
            }

            ticker.advance(Duration.ofMillis(11));
            try (FileSystemWrapper.FileChannelLease readLease = wrapper.getReadChannel(path);
                 FileSystemWrapper.FileChannelLease writeLease = wrapper.getWriteChannel(path)) {
                secondRead = readLease.getChannel();
                secondWrite = writeLease.getChannel();
                assertNotSame(firstRead, secondRead);
                assertNotSame(firstWrite, secondWrite);
            }

            assertFalse(firstRead.isOpen());
            assertFalse(firstWrite.isOpen());
            assertTrue(secondRead.isOpen());
            assertTrue(secondWrite.isOpen());
        }

        assertFalse(secondRead.isOpen());
        assertFalse(secondWrite.isOpen());
    }

    @Test
    public void testExpirationDefersCloseWhileChannelIsLeased() throws Exception {
        Path path = temporaryFolder.newFile().toPath();
        TestTicker ticker = new TestTicker();

        try (FileSystemWrapper wrapper = new FileSystemWrapper(10, 10, Duration.ofMillis(10), ticker)) {
            FileChannel firstChannel;
            try (FileSystemWrapper.FileChannelLease firstLease = wrapper.getReadChannel(path)) {
                firstChannel = firstLease.getChannel();
                ticker.advance(Duration.ofMillis(11));

                try (FileSystemWrapper.FileChannelLease secondLease = wrapper.getReadChannel(path)) {
                    assertNotSame(firstChannel, secondLease.getChannel());
                }
                assertTrue(firstChannel.isOpen());
            }

            assertFalse(firstChannel.isOpen());
        }
    }

    @Test
    public void testScheduledCleanupClosesIdleChannel() throws Exception {
        Path path = temporaryFolder.newFile().toPath();

        try (FileSystemWrapper wrapper = new FileSystemWrapper(10, 10, Duration.ofMillis(25))) {
            FileChannel channel;
            try (FileSystemWrapper.FileChannelLease lease = wrapper.getReadChannel(path)) {
                channel = lease.getChannel();
            }

            AssertExtensions.assertEventuallyEquals(false, channel::isOpen, 2000);
        }
    }

    @Test
    public void testSizeEvictionDefersCloseWhileChannelIsLeased() throws Exception {
        Path firstPath = temporaryFolder.newFile().toPath();
        Path secondPath = temporaryFolder.newFile().toPath();
        FileChannel firstChannel;

        try (FileSystemWrapper wrapper = new FileSystemWrapper(1, 1, Duration.ofMinutes(10));
             FileSystemWrapper.FileChannelLease firstLease = wrapper.getReadChannel(firstPath)) {
            firstChannel = firstLease.getChannel();
            try (FileSystemWrapper.FileChannelLease ignored = wrapper.getReadChannel(secondPath)) {
                assertTrue(firstChannel.isOpen());
            }
        }

        assertFalse(firstChannel.isOpen());
    }

    @Test
    public void testCloseDefersChannelCloseUntilLeaseIsReleased() throws Exception {
        Path path = temporaryFolder.newFile().toPath();
        FileSystemWrapper wrapper = new FileSystemWrapper(10, 10, Duration.ofMinutes(10));
        FileChannel channel;

        try {
            try (FileSystemWrapper.FileChannelLease lease = wrapper.getReadChannel(path)) {
                channel = lease.getChannel();
                wrapper.close();
                assertTrue(channel.isOpen());
                AssertExtensions.assertThrows("A closed wrapper must reject new leases.",
                        () -> wrapper.getReadChannel(path), ex -> ex instanceof IOException);
            }
            assertFalse(channel.isOpen());
        } finally {
            wrapper.close();
        }
    }

    private static class TestTicker extends Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }
}
