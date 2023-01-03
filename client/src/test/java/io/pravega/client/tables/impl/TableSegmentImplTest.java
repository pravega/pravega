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
package io.pravega.client.tables.impl;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.NoSuchKeyException;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableSegmentImpl} class.
 */
public class TableSegmentImplTest extends ThreadPooledTestSuite {
    private static final Segment SEGMENT = new Segment("scope", "kvt", 0);
    private static final PravegaNodeUri URI = new PravegaNodeUri("endpoint", 12345);
    private static final long SHORT_TIMEOUT = 2000;

    /**
     * Tests the {@link TableSegmentImpl#put} method in the following situations:
     * - Successful invocation.
     * - Conditional update failure ({@link BadKeyVersionException} and {@link NoSuchKeyException}).
     * - Connection reset failures are checked in {@link #testReconnect()}.
     */
    @Test
    public void testPut() throws Exception {
        val testEntries = Arrays.asList(
                notExistsEntry(1L, "one"),
                unversionedEntry(2L, "two"),
                versionedEntry(3L, "three", 123L));
        val expectedVersions = Arrays.asList(0L, 1L, 2L);
        val expectedWireEntries = toWireEntries(testEntries, testEntries.stream()
                .map(TableSegmentEntry::getKey)
                .map(TableSegmentKey::getKey)
                .map(this::keyFromBuf)
                .collect(Collectors.toList()));

        @Cleanup
        val context = new TestContext();

        // Successful operation.
        val putResult = context.segment.put(testEntries.iterator());
        val wireCommand = (WireCommands.UpdateTableEntries) context.getConnection().getLastSentWireCommand();
        checkWireCommand(expectedWireEntries, wireCommand.getTableEntries());

        context.sendReply(new WireCommands.TableEntriesUpdated(context.getConnection().getLastRequestId(), expectedVersions));
        val actualVersions = putResult.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS)
                .stream().map(TableSegmentKeyVersion::getSegmentVersion).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected return value", expectedVersions, actualVersions, Long::equals);

        // Conditional update failures.
        checkBadKeyVersion(context, s -> s.put(testEntries.iterator()));
        checkNoSuchKey(context, s -> s.put(testEntries.iterator()));
    }

    /**
     * Tests the {@link TableSegmentImpl#remove} method in the following situations:
     * - Successful invocation.
     * - Conditional update failure ({@link BadKeyVersionException} and {@link NoSuchKeyException}).
     * - Connection reset failures are checked in {@link #testReconnect()}.
     */
    @Test
    public void testRemove() throws Exception {
        val testKeys = Arrays.asList(
                notExistsKey(1L),
                unversionedKey(2L),
                versionedKey(3L, 123L));
        val expectedWireKeys = toWireKeys(testKeys);

        @Cleanup
        val context = new TestContext();

        // Successful operation.
        val removeResult = context.segment.remove(testKeys.iterator());
        val wireCommand = (WireCommands.RemoveTableKeys) context.getConnection().getLastSentWireCommand();
        checkWireCommand(expectedWireKeys, wireCommand.getKeys());
        context.sendReply(new WireCommands.TableKeysRemoved(context.getConnection().getLastRequestId(), SEGMENT.getScopedName()));
        removeResult.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS);

        // Conditional update failures.
        checkBadKeyVersion(context, s -> s.remove(testKeys.iterator()));
        checkNoSuchKey(context, s -> s.remove(testKeys.iterator()));
    }

    /**
     * Tests the {@link TableSegmentImpl#get} method.
     * Connection reset failures are not tested here; they're checked in {@link #testReconnect()}.
     */
    @Test
    public void testGet() throws Exception {
        val requestKeys = Arrays.asList(100L, 200L, 300L);
        val expectedEntries = Arrays.asList(
                versionedEntry(requestKeys.get(0), "one hundred", 1L),
                versionedEntry(requestKeys.get(1), "two hundred", 2L),
                null); // This key does not exist.
        val expectedWireKeys = toWireKeys(requestKeys.stream().map(this::buf).map(TableSegmentKey::unversioned).collect(Collectors.toList()));
        val replyWireEntries = toWireEntries(expectedEntries, requestKeys);

        @Cleanup
        val context = new TestContext();

        // Successful operation.
        val getResult = context.segment.get(requestKeys.stream().map(this::buf).iterator());
        val wireCommand = (WireCommands.ReadTable) context.getConnection().getLastSentWireCommand();
        checkWireCommand(expectedWireKeys, wireCommand.getKeys());
        context.sendReply(new WireCommands.TableRead(context.getConnection().getLastRequestId(), SEGMENT.getScopedName(), replyWireEntries));
        val actualEntries = getResult.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS);
        AssertExtensions.assertListEquals("Unexpected return value", expectedEntries, actualEntries, this::entryEquals);
    }

    /**
     * Tests the {@link TableSegmentImpl#getEntryCount()} method.
     */
    @Test
    public void testGetEntryCount() throws Exception {
        @Cleanup
        val context = new TestContext();
        val getInfoResult = context.segment.getEntryCount();
        val wireCommand = (WireCommands.GetTableSegmentInfo) context.getConnection().getLastSentWireCommand();
        Assert.assertEquals(SEGMENT.getKVTScopedName(), wireCommand.getSegmentName());
        context.sendReply(new WireCommands.TableSegmentInfo(context.getConnection().getLastRequestId(), SEGMENT.getKVTScopedName(),
                1, 2, 3L, 4));
        val actualResult = getInfoResult.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected return value", 3L, (long) actualResult);
    }

    /**
     * Tests the {@link TableSegmentImpl#get} method when the response coming back from the server is truncated.
     * Connection reset failures are not tested here; they're checked in {@link #testReconnect()}.
     */
    @Test
    public void testGetALotOfKeys() throws Exception {
        val expectedBatchKeyLength = 15; // Manual calculation of TableSegmentImpl.MAX_GET_KEY_BATCH_SIZE.
        val requestKeys = LongStream.range(0, 61).mapToObj(l -> l * 100).collect(Collectors.toList());
        val expectedEntries = requestKeys.stream().map(key -> versionedEntry(key, key.toString(), key)).collect(Collectors.toList());
        expectedEntries.set(expectedEntries.size() - 1, null); // This key does not exist.
        val expectedWireKeys = toWireKeys(requestKeys.stream().map(this::buf).map(TableSegmentKey::unversioned).collect(Collectors.toList()));

        // Calculate the expected sent/receive ranges.
        val expectedRanges = new ArrayList<Map.Entry<Integer, Integer>>(); // Key: Start Index, Value: End Index.
        int idx = 0;
        while (idx < expectedEntries.size()) {
            val expectedLength = Math.min(expectedBatchKeyLength, expectedEntries.size() - idx);
            expectedRanges.add(new AbstractMap.SimpleImmutableEntry<>(idx, idx + expectedLength));
            idx += expectedLength;
        }

        @Cleanup
        val context = new TestContext();

        // Initiate a get request.
        val getResult = context.segment.get(requestKeys.stream().map(this::buf).iterator());

        // Capture all the requests sent over the wire and verify that we've sent as many as we expected to.
        val sentWireCommands = context.getConnection().getLastSentWireCommands(expectedRanges.size());
        Assert.assertEquals(expectedRanges.size(), sentWireCommands.size());

        // Validate the sent keys and send appropriate responses.
        for (int i = 0; i < expectedRanges.size(); i++) {
            int rangeStart = expectedRanges.get(i).getKey();
            int rangeEnd = expectedRanges.get(i).getValue();

            val expectedSentKeys = expectedWireKeys.subList(rangeStart, rangeEnd);
            val wireCommand = (WireCommands.ReadTable) sentWireCommands.get(i);
            checkWireCommand(expectedSentKeys, wireCommand.getKeys());

            val returnedKeys = requestKeys.subList(rangeStart, rangeEnd);
            val returnedEntries = expectedEntries.subList(rangeStart, rangeEnd);
            val replyWireEntries = toWireEntries(returnedEntries, returnedKeys);
            context.sendReply(new WireCommands.TableRead(wireCommand.getRequestId(), SEGMENT.getScopedName(), replyWireEntries));
        }

        // Validate final result.
        val actualEntries = getResult.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS);
        AssertExtensions.assertListEquals("Unexpected return value", expectedEntries, actualEntries, this::entryEquals);
    }

    /**
     * Tests the {@link TableSegmentImpl#keyIterator} method.
     * Connection reset failures are not tested here; they're checked in {@link #testReconnect()}.
     */
    @Test
    public void testKeyIterator() throws Exception {
        @Cleanup
        val context = new TestContext();
        testIterator(context.segment::keyIterator,
                () -> ((WireCommands.ReadTableKeys) context.getConnection().getLastSentWireCommand()).getArgs().getFromKey(),
                () -> ((WireCommands.ReadTableKeys) context.getConnection().getLastSentWireCommand()).getArgs().getToKey(),
                TableSegmentEntry::getKey,
                expectedResult -> {
                    val replyKeys = toWireKeys(expectedResult);
                    context.sendReply(
                            new WireCommands.TableKeysRead(context.getConnection().getLastRequestId(), SEGMENT.getScopedName(), replyKeys, Unpooled.EMPTY_BUFFER));
                },
                this::keyEquals);
    }

    /**
     * Tests the {@link TableSegmentImpl#entryIterator} method.
     * Connection reset failures are not tested here; they're checked in {@link #testReconnect()}.
     */
    @Test
    public void testEntryIterator() throws Exception {
        @Cleanup
        val context = new TestContext();
        testIterator(context.segment::entryIterator,
                () -> ((WireCommands.ReadTableEntries) context.getConnection().getLastSentWireCommand()).getArgs().getFromKey(),
                () -> ((WireCommands.ReadTableEntries) context.getConnection().getLastSentWireCommand()).getArgs().getToKey(),
                e -> e,
                expectedResult -> {
                    val replyEntries = toWireEntries(expectedResult, null);
                    context.sendReply(
                            new WireCommands.TableEntriesRead(context.getConnection().getLastRequestId(), SEGMENT.getScopedName(), replyEntries, Unpooled.EMPTY_BUFFER));
                },
                this::entryEquals);
    }

    private <T> void testIterator(Function<SegmentIteratorArgs, AsyncIterator<IteratorItem<T>>> newIterator,
                                  Supplier<ByteBuf> getLastRequestFromKey,
                                  Supplier<ByteBuf> getLastRequestToKey,
                                  Function<TableSegmentEntry, T> getItemFromEntry,
                                  Consumer<List<T>> sendReply,
                                  BiPredicate<T, T> checkItemEquality) throws Exception {
        val suggestedKeyCount = 3;

        // Generate 100 Entries and split them into batches.
        val allEntries = IntStream.range(0, 100)
                                  .mapToObj(i -> versionedEntry(i * 10L, Integer.toString(i * 10), 1L))
                                  .collect(Collectors.toList());
        val inputEntries = splitIteratorInputs(allEntries);
        inputEntries.add(allEntries); // Do an full iteration as well.

        // Check regular iteration.
        for (int i = 0; i < inputEntries.size(); i++) {
            val entryList = inputEntries.get(i);
            SegmentIteratorArgs args = SegmentIteratorArgs.builder()
                    .fromKey(entryList.get(0).getKey().getKey())
                    .toKey(entryList.get(entryList.size() - 1).getKey().getKey())
                    .maxItemsAtOnce(suggestedKeyCount)
                    .build();
            val actualItems = new ArrayList<T>(); // We collect iterated items in this list.
            val itemsToReturn = entryList.iterator();
            val tableIterator = newIterator.apply(args);
            while (itemsToReturn.hasNext()) {
                val iteratorFuture = tableIterator.getNext();

                // Verify the wire command got sent as expected.
                val requestFromKey = getLastRequestFromKey.get();
                Assert.assertEquals("Unexpected fromKey sent.", args.getFromKey(), requestFromKey);
                val requestToKey = getLastRequestToKey.get();
                Assert.assertEquals("Unexpected toKey sent.", args.getToKey(), requestToKey);

                // Send a reply.
                val expectedResult = new ArrayList<T>();
                int count = suggestedKeyCount;
                while (itemsToReturn.hasNext() && count > 0) {
                    val next = itemsToReturn.next();
                    expectedResult.add(getItemFromEntry.apply(next));
                    args = args.next(next.getKey().getKey());
                    count--;
                }
                sendReply.accept(expectedResult);

                // Check the partial result.
                val iteratorResult = iteratorFuture.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS);
                AssertExtensions.assertListEquals("Unexpected partial result.", expectedResult, iteratorResult.getItems(), checkItemEquality);
                actualItems.addAll(iteratorResult.getItems());
            }

            // Then the final result.
            val expected = entryList.stream().map(getItemFromEntry).collect(Collectors.toList());
            AssertExtensions.assertListEquals("Unexpected result.", expected, actualItems, checkItemEquality);
        }
    }

    /**
     * Tests the ability to reconnect in the following situations:
     * - {@link ConnectionFailedException}
     * - {@link AuthenticationException})
     * - Unexpected replies.
     * <p>
     * These should result in automatic retries and reconnects up to a certain point.
     */
    @Test
    public void testReconnect() throws Exception {
        val keys = Arrays.asList(100L, 200L, 300L);
        val entries = Arrays.asList(
                versionedEntry(keys.get(0), "one hundred", 1L),
                versionedEntry(keys.get(1), "two hundred", 2L),
                versionedEntry(keys.get(2), "three hundred", 3L));
        val versions = entries.stream().map(e -> e.getKey().getVersion().getSegmentVersion()).collect(Collectors.toList());

        // All retryable replies. WrongHost and AuthTokenCheckFailed are converted into exceptions by RawClient, while all
        // others need to be handled by TableSegmentImpl.handleReply.
        val failureReplies = Arrays.<Function<Long, Reply>>asList(
                requestId -> new WireCommands.WrongHost(requestId, SEGMENT.getScopedName(), "NewHost", ""),
                requestId -> new WireCommands.AuthTokenCheckFailed(requestId, "", WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED),
                requestId -> new WireCommands.OperationUnsupported(requestId, "Intentional", ""));

        for (val fr : failureReplies) {
            // TableSegment.put()
            testConnectionFailure(ts -> ts.put(entries.get(0)), fr,
                    requestId -> new WireCommands.TableEntriesUpdated(requestId, versions),
                    result -> Assert.assertEquals("", (long) versions.get(0), result.getSegmentVersion()));

            // TableSegment.get()
            val wireEntries = entries.subList(0, 1);
            testConnectionFailure(ts -> ts.get(buf(keys.get(0))), fr,
                    requestId -> new WireCommands.TableRead(requestId, SEGMENT.getScopedName(), toWireEntries(wireEntries, null)),
                    result -> Assert.assertTrue("", entryEquals(wireEntries.get(0), result)));

            // TableSegment.remove()
            testConnectionFailure(ts -> ts.remove(unversionedKey(keys.get(0))), fr,
                    requestId -> new WireCommands.TableKeysRemoved(requestId, SEGMENT.getScopedName()),
                    result -> {
                    });

            // Iterators. It is sufficient to test one of them.
            val args = SegmentIteratorArgs.builder().maxItemsAtOnce(1)
                    .fromKey(Unpooled.wrappedBuffer(new byte[]{1}))
                    .toKey(Unpooled.wrappedBuffer(new byte[]{1}))
                    .build();
            testConnectionFailure(ts -> ts.entryIterator(args).getNext(), fr,
                    requestId -> new WireCommands.TableEntriesRead(requestId, SEGMENT.getScopedName(), toWireEntries(entries, null), Unpooled.wrappedBuffer(new byte[1])),
                    result -> AssertExtensions.assertListEquals("", entries, result.getItems(), this::entryEquals));
        }
    }

    private <T extends Reply, U> void testConnectionFailure(
            Function<TableSegment, CompletableFuture<U>> toInvoke,
            Function<Long, T> createFailureReply,
            Function<Long, T> createSuccessfulReply,
            Consumer<U> checkResult) throws Exception {
        @Cleanup
        val context = new TestContext();

        // Initiate the call.
        val result = toInvoke.apply(context.segment);

        // Fail the request. This should result in a retry.
        val connection1 = context.getConnection();
        context.sendReply(createFailureReply.apply(connection1.getLastRequestId()));

        // Wait for the connection to be closed.
        TestUtils.await(connection1::isClosed, 10, SHORT_TIMEOUT);

        // Wait for the retry to be initiated and sent
        TestUtils.await(() -> context.getConnection().getLastSentWireCommand() != null, 10, SHORT_TIMEOUT);

        // Send the successful reply.
        context.sendReply(createSuccessfulReply.apply(context.getConnection().getLastRequestId()));

        // Await the result.
        val resultContents = result.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS);
        checkResult.accept(resultContents);
        Assert.assertEquals("Unexpected number of connection attempts.", 2, context.getReconnectCount());
    }

    /**
     * Tests the ability to separate read and write requests on their own connections.
     */
    @Test
    public void testDualConnections() throws Exception {
        val testEntries = Arrays.asList(
                notExistsEntry(1L, "one"),
                unversionedEntry(2L, "two"),
                versionedEntry(3L, "three", 123L));
        val expectedVersions = Arrays.asList(0L, 1L, 2L);

        // No retry attempts - this will speed up the testing.
        val config = KeyValueTableClientConfiguration.builder().retryAttempts(1).build();

        @Cleanup
        val context = new TestContext(config);
        context.autoReconnect.set(false); // Disable auto-reconnects; this will make our testing life easier.

        // Successful operation. This should cause the "write" connection to be established (the "read" connection is not, yet).
        // We need to do this because MockConnectionFactoryImpl only supports one outstanding connection per URI.
        val put1 = context.segment.put(testEntries.iterator());
        context.sendReply(new WireCommands.TableEntriesUpdated(context.getConnection().getLastRequestId(), expectedVersions));
        val result1 = put1.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS)
                .stream().map(TableSegmentKeyVersion::getSegmentVersion).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected return value", expectedVersions, result1, Long::equals);

        // Close the active connection and disable auto-reconnects.
        context.getConnection().close();
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected update operation to fail when write connection forcibly closed.",
                () -> context.segment.put(testEntries.iterator()),
                ex -> ex instanceof RetriesExhaustedException
                        && Exceptions.unwrap(ex.getCause()) instanceof ConnectionFailedException);

        // Reestablish a connection, and do a read request to set up the "read" context.
        context.autoReconnect.set(true);
        context.reconnect();
        val getResult = context.segment.get(Iterators.singletonIterator(Unpooled.wrappedBuffer(new byte[1])));
        context.sendReply(new WireCommands.TableRead(context.getConnection().getLastRequestId(), SEGMENT.getScopedName(), new WireCommands.TableEntries(Collections.emptyList())));
        val actualEntries = getResult.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, actualEntries.size());

        // Again, close the active connection. Verify that read requests fail.
        context.getConnection().close();
        context.autoReconnect.set(false);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected update operation to fail when write connection forcibly closed.",
                () -> context.segment.get(Iterators.singletonIterator(Unpooled.wrappedBuffer(new byte[1]))),
                ex -> ex instanceof RetriesExhaustedException
                        && Exceptions.unwrap(ex.getCause()) instanceof ConnectionFailedException);
    }

    /**
     * Tests the ability to swiftly reject requests with Key or Value lengths exceeding predefined limits.
     */
    @Test
    public void testTooLongKeysOrValues() throws Exception {
        @Cleanup
        val context = new TestContext();

        // Check all operations that accept keys with keys that exceed length limits.
        val exceedsKeyLength = TableSegmentEntry.unversioned(new byte[TableSegment.MAXIMUM_KEY_LENGTH + 1], new byte[10]);
        AssertExtensions.assertSuppliedFutureThrows(
                "put() accepted Key exceeding length limit.",
                () -> context.segment.put(Iterators.singletonIterator(exceedsKeyLength)),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "remove() accepted Key exceeding length limit.",
                () -> context.segment.remove(Iterators.singletonIterator(exceedsKeyLength.getKey())),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertSuppliedFutureThrows(
                "get() accepted Key exceeding length limit.",
                () -> context.segment.get(exceedsKeyLength.getKey().getKey()),
                ex -> ex instanceof IllegalArgumentException);

        // Check all operations that accept entries with values that exceed length limits.
        val exceedsValueLength = TableSegmentEntry.unversioned(new byte[10], new byte[TableSegment.MAXIMUM_VALUE_LENGTH + 1]);
        AssertExtensions.assertSuppliedFutureThrows(
                "put() accepted Value exceeding length limit.",
                () -> context.segment.put(Iterators.singletonIterator(exceedsValueLength)),
                ex -> ex instanceof IllegalArgumentException);

        // Check that an entry update that has both Key and Value at the limit goes through.
        val limitEntry = TableSegmentEntry.unversioned(new byte[TableSegment.MAXIMUM_KEY_LENGTH], new byte[TableSegment.MAXIMUM_VALUE_LENGTH]);
        val putResult = context.segment.put(Iterators.singletonIterator(limitEntry));
        val wireCommand = (WireCommands.UpdateTableEntries) context.getConnection().getLastSentWireCommand();
        val expectedWireEntries = toWireEntries(Collections.singletonList(limitEntry), null);
        checkWireCommand(expectedWireEntries, wireCommand.getTableEntries());
        context.sendReply(new WireCommands.TableEntriesUpdated(context.getConnection().getLastRequestId(), Collections.singletonList(1L)));
        val actualVersion = putResult.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS).get(0).getSegmentVersion();
        Assert.assertEquals("Unexpected return value", 1L, actualVersion);
    }

    private void checkBadKeyVersion(TestContext context, Function<TableSegment, CompletableFuture<?>> action) {
        checkConditionalUpdateFailure(context,
                action,
                requestId -> new WireCommands.TableKeyBadVersion(context.getConnection().getLastRequestId(), SEGMENT.getScopedName(), ""),
                BadKeyVersionException.class);
    }

    private void checkNoSuchKey(TestContext context, Function<TableSegment, CompletableFuture<?>> action) {
        checkConditionalUpdateFailure(context,
                action,
                requestId -> new WireCommands.TableKeyDoesNotExist(context.getConnection().getLastRequestId(), SEGMENT.getScopedName(), ""),
                NoSuchKeyException.class);
    }

    private void checkConditionalUpdateFailure(TestContext context, Function<TableSegment, CompletableFuture<?>> action,
                                               Function<Long, Reply> getReply, Class<? extends Throwable> expectedException) {
        val result = action.apply(context.segment);
        context.sendReply(getReply.apply(context.getConnection().getLastRequestId()));

        AssertExtensions.assertThrows(
                "Expected the call to fail.",
                () -> result.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS),
                ex -> ex.getClass().equals(expectedException));
    }

    private void checkWireCommand(List<WireCommands.TableKey> expected, List<WireCommands.TableKey> actual) {
        AssertExtensions.assertListEquals("WireCommands.TableKey mismatch.", expected, actual,
                (e, a) -> e.getKeyVersion() == a.getKeyVersion()
                        && bufEquals(e.getData(), a.getData()));
    }

    private void checkWireCommand(WireCommands.TableEntries expected, WireCommands.TableEntries actual) {
        AssertExtensions.assertListEquals("WireCommands.TableEntries mismatch.", expected.getEntries(), actual.getEntries(),
                (e, a) -> e.getKey().getKeyVersion() == a.getKey().getKeyVersion()
                        && bufEquals(e.getKey().getData(), a.getKey().getData())
                        && bufEquals(e.getValue().getData(), a.getValue().getData()));
    }

    private List<WireCommands.TableKey> toWireKeys(List<TableSegmentKey> keys) {
        return keys.stream()
                .map(k -> new WireCommands.TableKey(k.getKey(), k.getVersion().getSegmentVersion()))
                .collect(Collectors.toList());
    }

    private WireCommands.TableEntries toWireEntries(List<TableSegmentEntry> entries, List<Long> keys) {
        val result = new ArrayList<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>>();
        for (int i = 0; i < entries.size(); i++) {
            val e = entries.get(i);
            WireCommands.TableKey key;
            WireCommands.TableValue value;
            if (e == null) {
                key = new WireCommands.TableKey(buf(keys.get(i)), TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion());
                value = new WireCommands.TableValue(Unpooled.EMPTY_BUFFER);
            } else {
                key = new WireCommands.TableKey(e.getKey().getKey(), e.getKey().getVersion().getSegmentVersion());
                value = new WireCommands.TableValue(e.getValue());
            }
            result.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
        }

        return new WireCommands.TableEntries(result);
    }

    private List<List<TableSegmentEntry>> splitIteratorInputs(List<TableSegmentEntry> entries) {
        val result = new ArrayList<List<TableSegmentEntry>>();
        int inputSize = 1;
        int index = 0;
        while (index < entries.size()) {
            val split = new ArrayList<TableSegmentEntry>();
            int upperBound = Math.min(entries.size(), index + inputSize);
            for (int i = index; i < upperBound; i++) {
                split.add(entries.get(i));
            }

            result.add(split);
            index += inputSize;
            inputSize++;
        }

        return result;
    }

    private boolean entryEquals(TableSegmentEntry a, TableSegmentEntry b) {
        if (a == null) {
            return b == null;
        } else if (b == null) {
            return false;
        } else {
            return keyEquals(a.getKey(), b.getKey()) && a.getValue().equals(b.getValue());
        }
    }

    private boolean keyEquals(TableSegmentKey a, TableSegmentKey b) {
        return a.getKey().equals(b.getKey())
                && a.getVersion().getSegmentVersion() == b.getVersion().getSegmentVersion();
    }

    private boolean bufEquals(ByteBuf expected, ByteBuf actual) {
        return expected.compareTo(actual) == 0;
    }

    private ByteBuf buf(long value) {
        ByteBuf b = Unpooled.wrappedBuffer(new byte[Long.BYTES]);
        b.setLong(0, value);
        return b;
    }

    private ByteBuf buf(String value) {
        return Unpooled.wrappedBuffer(value.getBytes(Charsets.UTF_8));
    }

    private long keyFromBuf(ByteBuf buf) {
        return buf.getLong(0);
    }

    private TableSegmentEntry versionedEntry(long key, String value, long version) {
        return TableSegmentEntry.versioned(buf(key), buf(value), version);
    }

    private TableSegmentEntry notExistsEntry(long key, String value) {
        return TableSegmentEntry.notExists(buf(key), buf(value));
    }

    private TableSegmentEntry unversionedEntry(long key, String value) {
        return TableSegmentEntry.unversioned(buf(key), buf(value));
    }

    private TableSegmentKey versionedKey(long key, long version) {
        return TableSegmentKey.versioned(buf(key), version);
    }

    private TableSegmentKey notExistsKey(long key) {
        return TableSegmentKey.notExists(buf(key));
    }

    private TableSegmentKey unversionedKey(long key) {
        return TableSegmentKey.unversioned(buf(key));
    }

    private class TestContext implements AutoCloseable {
        final MockConnectionFactoryImpl connectionFactory;
        final MockController controller;
        final AtomicReference<MockConnection> connection;
        final TableSegment segment;
        final AtomicBoolean autoReconnect = new AtomicBoolean(true);
        final AtomicInteger reconnectCount = new AtomicInteger();

        TestContext() {
            this(KeyValueTableClientConfiguration.builder().build());
        }

        TestContext(KeyValueTableClientConfiguration config) {
            this.connection = new AtomicReference<>();
            this.connectionFactory = new MockConnectionFactoryImpl();
            this.connectionFactory.setExecutor(executorService());
            this.controller = new MockController(URI.getEndpoint(), URI.getPort(), this.connectionFactory, true);
            val factory = new TableSegmentFactoryImpl(this.controller, this.connectionFactory, config,
                    DelegationTokenProviderFactory.createWithEmptyToken());
            this.segment = factory.forSegment(SEGMENT);
            reconnect();
        }

        @Override
        public void close() {
            this.autoReconnect.set(false);
            this.segment.close();
            getConnection().close();
            this.controller.close();
            this.connectionFactory.close();
        }

        MockConnection getConnection() {
            return this.connection.get();
        }

        int getReconnectCount() {
            return this.reconnectCount.get();
        }

        void sendReply(Reply reply) {
            val processor = this.connectionFactory.getProcessor(URI);
            if (reply instanceof WireCommands.AuthTokenCheckFailed) {
                processor.authTokenCheckFailed((WireCommands.AuthTokenCheckFailed) reply);
            } else {
                processor.process(reply);
            }
        }

        private void reconnect() {
            if (!this.autoReconnect.get()) {
                return;
            }

            val connection = new MockConnection(this::reconnect);
            this.connection.set(connection);
            this.connectionFactory.provideConnection(URI, connection);
            this.reconnectCount.incrementAndGet();
        }
    }

    @RequiredArgsConstructor
    private static class MockConnection implements ClientConnection {
        private final Runnable onClose;
        @Getter
        private long lastRequestId;
        private final Deque<WireCommand> lastSentWireCommands = new ArrayDeque<>();
        @Setter
        private boolean failed = false;
        @Getter
        private boolean closed = false;

        public WireCommand getLastSentWireCommand() {
            return this.lastSentWireCommands.isEmpty() ? null : this.lastSentWireCommands.peekLast();
        }

        public List<WireCommand> getLastSentWireCommands(int count) {
            val result = new ArrayList<WireCommand>();
            val it = this.lastSentWireCommands.descendingIterator();
            while (count > 0 && it.hasNext()) {
                result.add(it.next());
            }
            return Lists.reverse(result);
        }

        @Override
        public void close() {
            if (this.onClose != null) {
                this.onClose.run();
            }
            this.closed = true;
        }

        @Override
        public PravegaNodeUri getLocation() {
            return null;
        }

        @Override
        public void send(WireCommand cmd) throws ConnectionFailedException {
            if (this.closed) {
                throw new ConnectionFailedException("Connection closed");
            }

            this.lastRequestId = ((Request) cmd).getRequestId();
            this.lastSentWireCommands.addLast(cmd);
            if (this.failed) {
                throw new ConnectionFailedException();
            }
        }
        
        @Override
        public void send(Append append) {
            throw new UnsupportedOperationException("not needed for this test");
        }

        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {
            throw new UnsupportedOperationException("not needed for this test");
        }
    }
}
