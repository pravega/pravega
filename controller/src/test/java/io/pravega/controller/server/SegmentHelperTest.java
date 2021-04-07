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
package io.pravega.controller.server;

import io.netty.buffer.ByteBuf;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Getter;
import lombok.val;
import org.junit.Test;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.common.Exceptions.unwrap;
import static io.pravega.shared.NameUtils.getQualifiedStreamSegmentName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentHelperTest extends ThreadPooledTestSuite {

    private final byte[] key0 = "k".getBytes();
    private final byte[] key1 = "k1".getBytes();
    private final byte[] key2 = "k2".getBytes();
    private final byte[] key3 = "k3".getBytes();
    private final byte[] value = "v".getBytes();
    private final ByteBuf token1 = wrappedBuffer(new byte[]{0x01});
    private final ByteBuf token2 = wrappedBuffer(new byte[]{0x02});
    
    @Test
    public void getSegmentUri() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());

        helper.getSegmentUri("", "", 0);
    }

    @Test
    public void createSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<Void> retVal = helper.createSegment("", "",
                0, ScalingPolicy.fixed(2), "", Long.MIN_VALUE);
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> Exceptions.unwrap(ex) instanceof WireCommandFailedException
                        && ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.AuthFailed)
        );

        // On receiving SegmentAlreadyExists true should be returned.
        CompletableFuture<Void> result = helper.createSegment("", "", 0L, ScalingPolicy.fixed(2), "", requestId);
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentCreated(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        CompletableFuture<Void> ret = helper.createSegment("", "", 0L, ScalingPolicy.fixed(2), "", requestId);
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentAlreadyExists(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        ret.join();

        // handleUnexpectedReply
        CompletableFuture<Void> resultException = helper.createSegment("", "", 0L, ScalingPolicy.fixed(2), "", requestId);
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentDeleted(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        AssertExtensions.assertThrows("",
                () -> resultException.join(),
                ex -> ex instanceof ConnectionFailedException
        );

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.createSegment("", "",
                0, ScalingPolicy.fixed(2), "", Long.MIN_VALUE);
        validateProcessingFailureCFE(factory, futureSupplier);
        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void truncateSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<Void> retVal = helper.truncateSegment("", "", 0L, 0L,
                "", System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> Exceptions.unwrap(ex) instanceof WireCommandFailedException
                        && ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.AuthFailed)
        );

        CompletableFuture<Void> result = helper.truncateSegment("", "", 0L, 0L,
                "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentTruncated(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        result = helper.truncateSegment("", "", 0L, 0L,
                "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentIsTruncated(requestId, getQualifiedStreamSegmentName("", "", 0L), 0L, "", 0L));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.truncateSegment("", "", 0L, 0L,
                "", System.nanoTime());

        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void deleteSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<Void> retVal = helper.deleteSegment("", "", 0L, "", System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> Exceptions.unwrap(ex) instanceof WireCommandFailedException
                        && ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.AuthFailed)
        );

        CompletableFuture<Void> result = helper.deleteSegment("", "", 0L, "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentDeleted(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        result = helper.deleteSegment("", "", 0L, "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.NoSuchSegment(requestId, getQualifiedStreamSegmentName("", "", 0L), "", 0L));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.deleteSegment("", "", 0L, "", System.nanoTime());
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void sealSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<Void> retVal = helper.sealSegment("", "", 0L,
                "", System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> Exceptions.unwrap(ex) instanceof WireCommandFailedException
                        && ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.AuthFailed)
        );

        CompletableFuture<Void> result = helper.sealSegment("", "", 0L,
                "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentSealed(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        result = helper.sealSegment("", "", 0L,
                "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentIsSealed(requestId, getQualifiedStreamSegmentName("", "", 0L), "", 0L));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.sealSegment("", "", 0L,
                "", System.nanoTime());
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void createTransaction() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        UUID txId = new UUID(0, 0L);
        CompletableFuture<Void> retVal = helper.createTransaction("", "", 0L, txId,
                "");
        long requestId = ((MockConnection) (factory.connection)).getRequestId();

        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );

        CompletableFuture<Void> result = helper.createTransaction("", "", 0L, new UUID(0L, 0L), "");
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentCreated(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();
        
        result = helper.createTransaction("", "", 0L, new UUID(0L, 0L), "");
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentAlreadyExists(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.createTransaction("", "", 0L, txId,
                "");
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void commitTransaction() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<Controller.TxnStatus> retVal = helper.commitTransaction("", "", 0L, 0L, new UUID(0, 0L),
                "");
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );

        CompletableFuture<Controller.TxnStatus> result = helper.commitTransaction("", "", 0L, 0L, new UUID(0L, 0L), "");
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentsMerged(requestId, getQualifiedStreamSegmentName("", "", 0L), getQualifiedStreamSegmentName("", "", 0L), 0L));
        result.join();

        result = helper.commitTransaction("", "", 0L, 0L, new UUID(0L, 0L), "");
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.NoSuchSegment(requestId, getQualifiedStreamSegmentName("", "", 0L), "", 0L));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.commitTransaction("", "", 0L, 0L, new UUID(0, 0L),
                "");
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void abortTransaction() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<Controller.TxnStatus> retVal = helper.abortTransaction("", "", 0L, new UUID(0, 0L),
                "");
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );

        CompletableFuture<Controller.TxnStatus> result = helper.abortTransaction("", "", 1L, new UUID(0L, 0L), "");
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentDeleted(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        result = helper.abortTransaction("", "", 1L, new UUID(0L, 0L), "");
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.NoSuchSegment(requestId, getQualifiedStreamSegmentName("", "", 0L), "", 0L));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.abortTransaction("", "", 0L, new UUID(0, 0L),
                "");
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void updatePolicy() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<Void> retVal = helper.updatePolicy("", "", ScalingPolicy.fixed(1), 0L,
                "", System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );

        CompletableFuture<Void> result = helper.updatePolicy("", "", ScalingPolicy.fixed(1), 0L,
                "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentPolicyUpdated(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.updatePolicy("", "", ScalingPolicy.fixed(1), 0L,
                "", System.nanoTime());
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void getSegmentInfo() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        CompletableFuture<WireCommands.StreamSegmentInfo> retVal = helper.getSegmentInfo("", "", 0L,
                "");
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> Exceptions.unwrap(ex) instanceof WireCommandFailedException
        );

        CompletableFuture<WireCommands.StreamSegmentInfo> result = helper.getSegmentInfo("", "", 0L,
                "");
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.StreamSegmentInfo(requestId, getQualifiedStreamSegmentName("", "", 0L),
                true, true, true, 0L, 0L, 0L));
        result.join();

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.getSegmentInfo("", "", 0L,
                "");
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void testCreateTableSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        long requestId = Long.MIN_VALUE;

        // On receiving SegmentAlreadyExists true should be returned.
        CompletableFuture<Void> result = helper.createTableSegment("", "", requestId, false);
        requestId = ((MockConnection) (factory.connection)).getRequestId();

        factory.rp.process(new WireCommands.SegmentAlreadyExists(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        result.join();

        // On Receiving SegmentCreated true should be returned.
        result = helper.createTableSegment("", "", requestId, false);
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentCreated(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        // Validate failure conditions.
        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.createTableSegment("", "", 0L, true);
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateProcessingFailureCFE(factory, futureSupplier);
        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void testDeleteTableSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        long requestId = System.nanoTime();

        // On receiving NoSuchSegment true should be returned.
        CompletableFuture<Void> result = helper.deleteTableSegment("", true, "", requestId);
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.NoSuchSegment(requestId, getQualifiedStreamSegmentName("", "", 0L), "", -1L));
        result.join();

        // On receiving SegmentDeleted true should be returned.
        result = helper.deleteTableSegment("", true, "", requestId);
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.SegmentDeleted(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        result.join();

        // On receiving TableSegmentNotEmpty WireCommandFailedException is thrown.
        result = helper.deleteTableSegment("", true, "", requestId);
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.TableSegmentNotEmpty(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableSegmentNotEmpty));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.deleteTableSegment("", true, "", 0L);
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateProcessingFailureCFE(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void testUpdateTableEntries() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        List<TableSegmentEntry> entries = Arrays.asList(TableSegmentEntry.notExists("k".getBytes(), "v".getBytes()),
                TableSegmentEntry.unversioned("k1".getBytes(), "v".getBytes()),
                TableSegmentEntry.versioned("k2".getBytes(), "v".getBytes(), 10L));

        List<TableSegmentKeyVersion> expectedVersions = Arrays.asList(TableSegmentKeyVersion.from(0L),
                TableSegmentKeyVersion.from(1L),
                TableSegmentKeyVersion.from(11L));

        // On receiving TableEntriesUpdated.
        CompletableFuture<List<TableSegmentKeyVersion>> result = helper.updateTableEntries("", entries, "", System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.TableEntriesUpdated(requestId, Arrays.asList(0L, 1L, 11L)));
        assertEquals(expectedVersions, result.join());

        // On receiving TableKeyDoesNotExist.
        result = helper.updateTableEntries("", entries, "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.TableKeyDoesNotExist(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyDoesNotExist));

        // On receiving TableKeyBadVersion.
        result = helper.updateTableEntries("", entries, "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.TableKeyBadVersion(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyBadVersion));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.updateTableEntries("", entries, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateProcessingFailureCFE(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void testRemoveTableKeys() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        List<TableSegmentKey> keys = Arrays.asList(TableSegmentKey.notExists("k".getBytes()),
                TableSegmentKey.notExists("k1".getBytes()));

        // On receiving TableKeysRemoved.
        CompletableFuture<Void> result = helper.removeTableKeys("", keys, "",
                System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.TableKeysRemoved(requestId, getQualifiedStreamSegmentName("", "", 0L)));
        assertTrue(Futures.await(result));

        // On receiving TableKeyDoesNotExist.
        result = helper.removeTableKeys("", keys, "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();

        factory.rp.process(new WireCommands.TableKeyDoesNotExist(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        assertTrue(Futures.await(result));

        // On receiving TableKeyBadVersion.
        result = helper.removeTableKeys("", keys, "", System.nanoTime());
        requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.TableKeyBadVersion(requestId, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyBadVersion));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.removeTableKeys("", keys, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateProcessingFailureCFE(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void testReadTable() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        List<TableSegmentKey> keysToBeRead = Arrays.asList(TableSegmentKey.unversioned(key0),
                TableSegmentKey.unversioned(key1));

        List<TableSegmentEntry> responseFromSegmentStore = Arrays.asList(
                TableSegmentEntry.versioned(key0, value, 10L),
                TableSegmentEntry.notExists(key1, value));

        CompletableFuture<List<TableSegmentEntry>> result = helper.readTable("", keysToBeRead,
                "", System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.TableRead(requestId, getQualifiedStreamSegmentName("", "", 0L), getTableEntries(responseFromSegmentStore)));
        List<TableSegmentEntry> readResult = result.join();
        assertArrayByteBufEquals(key0, readResult.get(0).getKey().getKey());
        assertEquals(10L, readResult.get(0).getKey().getVersion().getSegmentVersion());
        assertArrayByteBufEquals(value, readResult.get(0).getValue());
        assertArrayByteBufEquals(key1, readResult.get(1).getKey().getKey());
        assertEquals(TableSegmentKeyVersion.NOT_EXISTS, readResult.get(1).getKey().getVersion());
        assertArrayByteBufEquals(value, readResult.get(1).getValue());

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.readTable("", keysToBeRead,
                "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateProcessingFailureCFE(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void testReadTableKeys() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());

        final List<TableSegmentKey> keys1 = Arrays.asList(
                TableSegmentKey.versioned(key0, 2L),
                TableSegmentKey.versioned(key1, 10L));

        final List<TableSegmentKey> keys2 = Arrays.asList(
                TableSegmentKey.versioned(key2, 2L),
                TableSegmentKey.versioned(key3, 10L));

        CompletableFuture<IteratorItem<TableSegmentKey>> result = helper.readTableKeys("", 3,
                IteratorStateImpl.EMPTY,
                "", System.nanoTime());

        assertFalse(result.isDone());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();

        factory.rp.process(getTableKeysRead(requestId, keys1, token1));
        assertTrue(Futures.await(result));
        // Validate the results.
        List<TableSegmentKey> iterationResult = result.join().getItems();
        assertArrayByteBufEquals(key0, iterationResult.get(0).getKey());
        assertEquals(2L, iterationResult.get(0).getVersion().getSegmentVersion());
        assertArrayByteBufEquals(key1, iterationResult.get(1).getKey());
        assertEquals(10L, iterationResult.get(1).getVersion().getSegmentVersion());
        assertArrayEquals(token1.array(), IteratorStateImpl.copyOf(result.join().getState()).getToken().array());

        // fetch the next value
        result = helper.readTableKeys("", 3, IteratorStateImpl.fromBytes(token1), "",
                System.nanoTime());
        assertFalse(result.isDone());
        requestId = ((MockConnection) (factory.connection)).getRequestId();

        factory.rp.process(getTableKeysRead(requestId, keys2, token2));
        assertTrue(Futures.await(result));
        // Validate the results.
        iterationResult = result.join().getItems();
        assertArrayByteBufEquals(key2, iterationResult.get(0).getKey());
        assertEquals(2L, iterationResult.get(0).getVersion().getSegmentVersion());
        assertArrayByteBufEquals(key3, iterationResult.get(1).getKey());
        assertEquals(10L, iterationResult.get(1).getVersion().getSegmentVersion());
        assertArrayEquals(token2.array(), IteratorStateImpl.copyOf(result.join().getState()).getToken().array());

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.readTableKeys("", 1,
                IteratorStateImpl.fromBytes(wrappedBuffer(new byte[0])),
                "", 0L);
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateProcessingFailureCFE(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test
    public void testReadTableEntries() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        List<TableSegmentEntry> entries1 = Arrays.asList(
                TableSegmentEntry.versioned(key0, value, 10L),
                TableSegmentEntry.versioned(key1, value, 10L));

        List<TableSegmentEntry> entries2 = Arrays.asList(
                TableSegmentEntry.versioned(key2, value, 10L),
                TableSegmentEntry.versioned(key3, value, 10L));

        CompletableFuture<IteratorItem<TableSegmentEntry>> result = helper.readTableEntries("", 3,
                null,
                "", System.nanoTime());
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        assertFalse(result.isDone());
        factory.rp.process(getTableEntriesRead(requestId, entries1, token1));
        result.join();
        List<TableSegmentEntry> iterationResult = result.join().getItems();
        assertArrayByteBufEquals(key0, iterationResult.get(0).getKey().getKey());
        assertEquals(10L, iterationResult.get(0).getKey().getVersion().getSegmentVersion());
        assertArrayByteBufEquals(value, iterationResult.get(0).getValue());
        assertArrayByteBufEquals(key1, iterationResult.get(1).getKey().getKey());
        assertEquals(10L, iterationResult.get(1).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(token1.array(), IteratorStateImpl.copyOf(result.join().getState()).getToken().array());

        result = helper.readTableEntries("", 3, IteratorStateImpl.fromBytes(token1), "",
                System.nanoTime());
        assertFalse(result.isDone());
        requestId = ((MockConnection) (factory.connection)).getRequestId();

        factory.rp.process(getTableEntriesRead(requestId, entries2, token2));
        assertTrue(Futures.await(result));
        iterationResult = result.join().getItems();
        assertArrayByteBufEquals(key2, iterationResult.get(0).getKey().getKey());
        assertEquals(10L, iterationResult.get(0).getKey().getVersion().getSegmentVersion());
        assertArrayByteBufEquals(value, iterationResult.get(0).getValue());
        assertArrayByteBufEquals(key3, iterationResult.get(1).getKey().getKey());
        assertEquals(10L, iterationResult.get(1).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(token2.array(), IteratorStateImpl.copyOf(result.join().getState()).getToken().array());

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.readTableEntries("", 1,
                IteratorStateImpl.fromBytes(wrappedBuffer(new byte[0])),
                "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateProcessingFailureCFE(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);

        testConnectionFailure(factory, futureSupplier);
    }

    @Test(timeout = 10000)
    public void testTimeout() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(factory, new MockHostControllerStore(), executorService());
        helper.setTimeout(Duration.ofMillis(100));
        List<TableSegmentKey> keysToBeRead = Arrays.asList(TableSegmentKey.unversioned(key0),
                TableSegmentKey.unversioned(key1));

        CompletableFuture<List<TableSegmentEntry>> result = helper.readTable("", keysToBeRead,
                "", System.nanoTime());
        
        AssertExtensions.assertFutureThrows("result should timeout", result, 
                e -> Exceptions.unwrap(e) instanceof WireCommandFailedException && 
                        ((WireCommandFailedException) Exceptions.unwrap(e)).getReason().equals(WireCommandFailedException.Reason.ConnectionFailed));
    }

    @Test
    public void testProcessAndRethrowExceptions() {
        // The wire-command itself we use for this test is immaterial, so we are using the simplest one here.
        WireCommands.Hello dummyRequest = new WireCommands.Hello(0, 0);
        SegmentHelper objectUnderTest = new SegmentHelper(null, null, null);

        AssertExtensions.assertThrows("Unexpected exception thrown",
                () -> objectUnderTest.<WireCommands.Hello>processAndRethrowException(1, dummyRequest,
                        new ExecutionException(new ConnectionFailedException())),
                e -> hasWireCommandFailedWithReason(e, WireCommandFailedException.Reason.ConnectionFailed));

        AssertExtensions.assertThrows("Unexpected exception thrown",
                () -> objectUnderTest.<WireCommands.Hello>processAndRethrowException(1, dummyRequest,
                        new ExecutionException(new AuthenticationException("Authentication failed"))),
                e -> hasWireCommandFailedWithReason(e, WireCommandFailedException.Reason.AuthFailed));

        AssertExtensions.assertThrows("Unexpected exception thrown",
                () -> objectUnderTest.<WireCommands.Hello>processAndRethrowException(1, dummyRequest,
                        new ExecutionException(new TokenExpiredException("Token expired"))),
                e -> hasWireCommandFailedWithReason(e, WireCommandFailedException.Reason.AuthFailed));

        AssertExtensions.assertThrows("Unexpected exception thrown",
                () -> objectUnderTest.<WireCommands.Hello>processAndRethrowException(1, dummyRequest,
                        new ExecutionException(new TimeoutException("Authentication failed"))),
                e -> hasWireCommandFailedWithReason(e, WireCommandFailedException.Reason.ConnectionFailed));

        AssertExtensions.assertThrows("Unexpected exception thrown",
                () -> objectUnderTest.<WireCommands.Hello>processAndRethrowException(1, dummyRequest,
                        new ExecutionException(new RuntimeException())),
                e -> e instanceof ExecutionException && e.getCause() instanceof RuntimeException);
    }

    private boolean hasWireCommandFailedWithReason(Throwable e, WireCommandFailedException.Reason reason) {
        if (e instanceof WireCommandFailedException) {
            WireCommandFailedException wrappedException = (WireCommandFailedException) e;
            return wrappedException.getReason().equals(reason);
        } else {
            return false;
        }
    }

    private WireCommands.TableEntries getTableEntries(List<TableSegmentEntry> entries) {
        return new WireCommands.TableEntries(entries.stream().map(e -> {
            val k = new WireCommands.TableKey(e.getKey().getKey(), e.getKey().getVersion().getSegmentVersion());
            val v = new WireCommands.TableValue(e.getValue());
            return new AbstractMap.SimpleImmutableEntry<>(k, v);
        }).collect(Collectors.toList()));
    }

    private WireCommands.TableKeysRead getTableKeysRead(long requestId, List<TableSegmentKey> keys, ByteBuf continuationToken) {
        return new WireCommands.TableKeysRead(requestId, getQualifiedStreamSegmentName("", "", 0L),
                keys.stream().map(e -> new WireCommands.TableKey(e.getKey(), e.getVersion().getSegmentVersion()))
                    .collect(Collectors.toList()),
                continuationToken);
    }

    private WireCommands.TableEntriesRead getTableEntriesRead(long requestId, List<TableSegmentEntry> entries, ByteBuf continuationToken) {
        return new WireCommands.TableEntriesRead(requestId, getQualifiedStreamSegmentName("", "", 0L),
                getTableEntries(entries), continuationToken);
    }

    private void validateAuthTokenCheckFailed(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.AuthTokenCheckFailed(requestId, "SomeException"));
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException && ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.AuthFailed);
                                      });
    }

    private void validateNoSuchSegment(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        long requestId = ((MockConnection) (factory.connection)).getRequestId();

        factory.rp.process(new WireCommands.NoSuchSegment(requestId, "segment", "SomeException", -1L));
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException && 
                                                  ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.SegmentDoesNotExist);
                                      });
    }

    private void validateWrongHost(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        long requestId = ((MockConnection) (factory.connection)).getRequestId();
        factory.rp.process(new WireCommands.WrongHost(requestId, "segment", "correctHost", "SomeException"));
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException &&
                                                  ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.ConnectionFailed);
                                      });
    }

    private void validateConnectionDropped(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.connectionDropped();
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException &&
                                                  ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.ConnectionFailed);
                                      });
    }

    private void validateProcessingFailure(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.processingFailure(new RuntimeException());
        AssertExtensions.assertThrows("", future::join, ex -> unwrap(ex) instanceof RuntimeException);
    }
    
    private void validateProcessingFailureCFE(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.processingFailure(new ConnectionFailedException());
        AssertExtensions.assertThrows("", future::join,
                t -> {
                    Throwable ex = unwrap(t);
                    return ex instanceof WireCommandFailedException &&
                            ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.ConnectionFailed);
                });
    }

    private void testConnectionFailure(MockConnectionFactory factory, Supplier<CompletableFuture<?>> future) {
        factory.failConnection.set(true);
        AssertExtensions.assertFutureThrows("",
                future.get(),
                ex -> ex instanceof WireCommandFailedException &&
                        ((WireCommandFailedException) ex).getReason().equals(WireCommandFailedException.Reason.ConnectionFailed));
    }

    private void assertArrayByteBufEquals(byte[] expected, ByteBuf actual) {
        // For all our tests, the ByteBuf is backed by arrays so we can make use of that.
        AssertExtensions.assertArrayEquals("", expected, 0, actual.array(), actual.arrayOffset(), expected.length);
    }

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    private static class MockHostControllerStore implements HostControllerStore {

        @Override
        public Map<Host, Set<Integer>> getHostContainersMap() {
            return null;
        }

        @Override
        public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {

        }

        @Override
        public int getContainerCount() {
            return 0;
        }

        @Override
        public Host getHostForSegment(String scope, String stream, long segmentId) {
            return new Host("localhost", 1000, "");
        }

        @Override
        public Host getHostForTableSegment(String table) {
            return new Host("localhost", 1000, "");
        }
    }

    private class MockConnectionFactory implements ConnectionFactory, ConnectionPool {
        private final AtomicBoolean failConnection = new AtomicBoolean(false);
        @Getter
        private ReplyProcessor rp;
        private ClientConnection connection;

        @Override
        public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
            if (failConnection.get()) {
                return Futures.failedFuture(new RuntimeException());   
            } else {
                this.rp = rp;
                this.connection = new MockConnection(rp, failConnection);
                return CompletableFuture.completedFuture(connection);
            }
        }

        @Override
        public CompletableFuture<ClientConnection> getClientConnection(Flow flow, PravegaNodeUri uri, ReplyProcessor rp) {
            this.rp = rp;
            this.connection = new MockConnection(rp, failConnection);
            return CompletableFuture.completedFuture(connection);
        }

        @Override
        public CompletableFuture<ClientConnection> getClientConnection(PravegaNodeUri uri, ReplyProcessor rp) {
            this.rp = rp;
            this.connection = new MockConnection(rp, failConnection);
            return CompletableFuture.completedFuture(connection);
        }

        @Override
        public void getClientConnection(Flow flow, PravegaNodeUri uri, ReplyProcessor rp, CompletableFuture<ClientConnection> connection) {
            this.rp = rp;
            this.connection = new MockConnection(rp, failConnection);
            connection.complete(this.connection);
        }

        @Override
        public ScheduledExecutorService getInternalExecutor() {
            return null;
        }

        @Override
        public void close() {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private class MockConnection implements ClientConnection {
        private final AtomicBoolean toFail;
        @Getter
        private final ReplyProcessor rp;
        @Getter
        private long requestId;

        public MockConnection(ReplyProcessor rp, AtomicBoolean toFail) {
            this.rp = rp;
            this.toFail = toFail;
        }

        @Override
        public void send(WireCommand cmd) throws ConnectionFailedException {
            this.requestId = ((Request) cmd).getRequestId();
            if (toFail.get()) {
                throw new ConnectionFailedException();
            }
        }

        @Override
        public void send(Append append) throws ConnectionFailedException {

        }


        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {

        }

        @Override
        public void close() {

        }
    }
}