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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;

import org.apache.bookkeeper.client.api.DigestType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the BookKeeperConfig class.
 */
public class BookKeeperConfigTest {

    @Test
    public void testDefaultValues() {
        BookKeeperConfig cfg = BookKeeperConfig.builder()
                .build();
        Assert.assertEquals("localhost:2181", cfg.getZkAddress());
        Assert.assertEquals(Duration.ofMillis(10000), cfg.getZkSessionTimeout());
        Assert.assertEquals(Duration.ofMillis(10000), cfg.getZkConnectionTimeout());
        Assert.assertEquals("/segmentstore/containers", cfg.getZkMetadataPath());
        Assert.assertEquals(2, cfg.getZkHierarchyDepth());
        Assert.assertEquals(5, cfg.getMaxWriteAttempts());
        Assert.assertEquals(3, cfg.getBkEnsembleSize());
        Assert.assertEquals(2, cfg.getBkAckQuorumSize());
        Assert.assertEquals(3, cfg.getBkWriteQuorumSize());
        Assert.assertEquals(60000, cfg.getBkWriteTimeoutMillis());
        Assert.assertEquals(30000, cfg.getBkReadTimeoutMillis());
        Assert.assertEquals(10000, cfg.getBkUserTcpTimeoutMillis());
        Assert.assertEquals(64, cfg.getBkReadBatchSize());
        Assert.assertEquals(256 * 1024 * 1024, cfg.getMaxOutstandingBytes());
        Assert.assertEquals(1024 * 1024 * 1024, cfg.getBkLedgerMaxSize());
        Assert.assertEquals(0, cfg.getBKPassword().length);
        Assert.assertEquals("", cfg.getBkLedgerPath());
        Assert.assertEquals(false, cfg.isTLSEnabled());
        Assert.assertEquals("config/client.truststore.jks", cfg.getTlsTrustStore());
        Assert.assertEquals("", cfg.getTlsTrustStorePasswordPath());
        Assert.assertEquals(false, cfg.isEnforceMinNumRacksPerWriteQuorum());
        Assert.assertEquals(2, cfg.getMinNumRacksPerWriteQuorum());
        Assert.assertEquals("/opt/pravega/scripts/sample-bookkeeper-topology.sh", cfg.getNetworkTopologyFileName());
        Assert.assertEquals(DigestType.CRC32C, cfg.getDigestType());
    }

    @Test
    public void testBadValues() {
        AssertExtensions.assertThrows(
                BookKeeperConfig.ZK_HIERARCHY_DEPTH.toString(),
                () -> BookKeeperConfig.builder().with(BookKeeperConfig.ZK_HIERARCHY_DEPTH, -1).build(),
                ex -> ex instanceof InvalidPropertyValueException);

        AssertExtensions.assertThrows(
                BookKeeperConfig.BK_WRITE_QUORUM_SIZE.toString(),
                () -> BookKeeperConfig.builder().with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 2)
                        .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 3).build(),
                ex -> ex instanceof InvalidPropertyValueException);

        AssertExtensions.assertThrows(
                BookKeeperConfig.BK_READ_BATCH_SIZE.toString(),
                () -> BookKeeperConfig.builder().with(BookKeeperConfig.BK_READ_BATCH_SIZE, -1).build(),
                ex -> ex instanceof InvalidPropertyValueException);
    }

    @Test
    public void testZkServers() {
        // When multiple servers are specified and separated by comma, it should replace it by semicolon
        BookKeeperConfig cfg1 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "foo:12345,bar:54321")
                .build();
        Assert.assertEquals("foo:12345;bar:54321", cfg1.getZkAddress());

        // No changes should be made to the following configs
        BookKeeperConfig cfg2 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "foo:12345")
                .build();
        Assert.assertEquals("foo:12345", cfg2.getZkAddress());

        BookKeeperConfig cfg3 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "10.20.30.40:12345;bar:2181")
                .build();
        Assert.assertEquals("10.20.30.40:12345;bar:2181", cfg3.getZkAddress());
    }

    @Test
    public void testQuorumSize() {
        AssertExtensions.assertThrows("BookKeeperConfig did not throw InvalidPropertyValueException",
                () -> BookKeeperConfig.builder()
                        .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 3)
                        .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 2)
                        .build(),
                ex -> ex instanceof InvalidPropertyValueException);
    }

    @Test
    public void testZkHierarchyDepth() {
        AssertExtensions.assertThrows("BookKeeperConfig did not throw InvalidPropertyValueException",
                () -> BookKeeperConfig.builder()
                        .with(BookKeeperConfig.ZK_HIERARCHY_DEPTH, -1)
                        .build(),
                ex -> ex instanceof InvalidPropertyValueException);
    }

    @Test
    public void testDigestTypeConversion() {
        Assert.assertEquals(DigestType.CRC32, BookKeeperConfig.getDigestType("CRC32"));
        Assert.assertEquals(DigestType.CRC32C, BookKeeperConfig.getDigestType("CRC32C"));
        Assert.assertEquals(DigestType.DUMMY, BookKeeperConfig.getDigestType("DUMMY"));
        Assert.assertEquals(DigestType.MAC, BookKeeperConfig.getDigestType("MAC"));
        Assert.assertEquals(DigestType.CRC32C, BookKeeperConfig.getDigestType("any-other-value"));
    }
}
