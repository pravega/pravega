/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.Timer;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.LogAddress;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import lombok.val;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Created by andrei on 4/14/17.
 */
public class Playground {
    public static void main(String[] args) throws Exception {
        //testLog();
    }

    private static void testLog() throws Exception {
        final int writeCount = 10;
        final Duration TIMEOUT = Duration.ZERO;
        val config = BookKeeperConfig.builder()
                                     .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, 17)
                                     .build();

        @Cleanup("shutdown")
        val executor = Executors.newScheduledThreadPool(10);
        @Cleanup
        BookKeeperLogFactory factory = new BookKeeperLogFactory(config, executor);
        factory.initialize();
        @Cleanup
        val log = factory.createDurableDataLog(1);
        log.initialize(TIMEOUT);
        System.out.println("Opened Log; LastSeqNo = " + log.getLastAppendSequence() + " Epoch = " + log.getEpoch());

        for (int i = 0; i < writeCount; i++) {
            byte[] data = ("append_" + i).getBytes();
            val t = new Timer();
            val address = log.append(new ByteArrayInputStream(data), TIMEOUT).join();
            System.out.println(String.format("Wrote %d bytes at address '%s' (%d ms): %s", data.length, address, t.getElapsedMillis(), new String(data)));
        }

        val addresses = new ArrayList<LogAddress>();
        @Cleanup
        val tReader = log.getReader(-1);
        DurableDataLog.ReadItem current;
        while ((current = tReader.getNext()) != null) {
            addresses.add(current.getAddress());
        }

        // Now read everything
        for (LogAddress address : addresses) {
            System.out.println();
            log.truncate(address, TIMEOUT).join();
            @Cleanup
            val reader = log.getReader(-1);
            while ((current = reader.getNext()) != null) {
                System.out.println(String.format("Read %d bytes from address %s: '%s'",
                        current.getPayload().length, current.getAddress(), new String(current.getPayload())));
            }
        }
        //
        //        for (LogAddress truncateAddress : addresses) {
        //            System.out.println();
        //            log.truncate(truncateAddress, TIMEOUT).join();
        ////
        ////            @Cleanup
        ////            val truncateReader = log.getReader(-1);
        ////            DurableDataLog.ReadItem current;
        ////            while ((current = truncateReader.getNext()) != null) {
        ////                System.out.println(String.format("Truncate @'%s': Read %d bytes from address %s: '%s'",
        ////                        truncateAddress,
        ////                        current.getPayload().length, current.getAddress(), new String(current.getPayload())));
        ////            }
        //        }
    }

    private static void testRaw() throws Exception {
        final String ZK_SERVER = "127.0.0.1:2181";
        final String CONTAINER_PATH = "/0";
        final byte[] PASSWORD = "pwd".getBytes();

        @Cleanup
        val mainCurator = CuratorFrameworkFactory.newClient(ZK_SERVER, 10000, 10000,
                new ExponentialBackoffRetry(1000, 3));
        mainCurator.start();
        mainCurator.blockUntilConnected();
        val curator = mainCurator.usingNamespace("pravega/containers");
        System.out.println("Connected to ZK");

        ClientConfiguration config = new ClientConfiguration()
                .setZkServers(ZK_SERVER)
                .setZkTimeout(30000);

        @Cleanup
        BookKeeper bk = new BookKeeper(config);
        System.out.println("Connected to BK");

        // Get current list of ledgers.
        Stat stat = new Stat();
        List<Long> ledgers;
        boolean mustCreate = false;
        try {
            byte[] ledgerListBytes = curator.getData()
                                            .storingStatIn(stat).forPath(CONTAINER_PATH);
            ledgers = listFromBytes(ledgerListBytes);
        } catch (KeeperException.NoNodeException nne) {
            ledgers = new ArrayList<>();
            mustCreate = true;
        }

        // List them out (TODO)
        for (long ledgerId : ledgers) {
            System.out.println(String.format("Ledger %s", ledgerId));
            LedgerHandle lh;
            try {
                lh = bk.openLedger(ledgerId, BookKeeper.DigestType.MAC, PASSWORD);
            } catch (BKException.BKLedgerRecoveryException e) {
                throw e;// TODO: Corruption?
            }

            Enumeration<LedgerEntry> entries = lh.readEntries(0, lh.getLastAddConfirmed());

            while (entries.hasMoreElements()) {
                LedgerEntry e = entries.nextElement();
                System.out.println(String.format("\t READ: LedgerId=%s, EntryId=%s, Data=%s", lh.getId(), e.getEntryId(), new String(e.getEntry())));
            }
        }

        // Create new ledger:
        LedgerHandle lh = bk.createLedger(3, 3, 2, BookKeeper.DigestType.MAC, PASSWORD);
        ledgers.add(lh.getId());
        byte[] ledgerListBytes = listToBytes(ledgers);
        if (mustCreate) {
            try {
                curator.create()
                       .creatingParentsIfNeeded()
                       .forPath(CONTAINER_PATH, ledgerListBytes);
            } catch (KeeperException.NodeExistsException nne) {
                lh.close();
                throw nne; // TODO: give up? someone else did this for us?
            }
        } else {
            try {
                curator.setData()
                       .withVersion(stat.getVersion())
                       .forPath(CONTAINER_PATH, ledgerListBytes);
            } catch (KeeperException.BadVersionException bve) {
                lh.close();
                throw bve; // TODO: concurrent access. Give up.
            }
        }

        byte[] writeData = String.format("Append: LH.Id=%s, LH.Length=%s.", lh.getId(), lh.getLength()).getBytes();
        //long entryId = lh.addEntry(writeData);
        long entryId = lh.addEntry(new byte[1024 * 1024 - 100]);
        System.out.println(String.format("WRITE: LedgerId=%s, EntryId=%s, Data=%s", lh.getId(), entryId, new String(writeData)));
        lh.close();
    }

    static byte[] listToBytes(List<Long> ledgerIds) {
        ByteBuffer bb = ByteBuffer.allocate((Long.SIZE * ledgerIds.size()) / 8);
        for (Long l : ledgerIds) {
            bb.putLong(l);
        }
        return bb.array();
    }

    static List<Long> listFromBytes(byte[] bytes) {
        List<Long> ledgerIds = new ArrayList<>();
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        while (bb.remaining() > 0) {
            ledgerIds.add(bb.getLong());
        }
        return ledgerIds;
    }
}
