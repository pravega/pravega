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

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import lombok.Getter;
import org.apache.bookkeeper.client.api.DigestType;

/**
 * General configuration for BookKeeper Client.
 */
public class BookKeeperConfig {
    //region Config Names

    public static final Property<String> ZK_ADDRESS = Property.named("zk.connect.uri", "localhost:2181", "zkAddress");
    public static final Property<Integer> ZK_SESSION_TIMEOUT = Property.named("zk.connect.sessionTimeout.milliseconds", 10000, "zkSessionTimeoutMillis");
    public static final Property<Integer> ZK_CONNECTION_TIMEOUT = Property.named("zk.connect.sessionTimeout.milliseconds", 10000, "zkConnectionTimeoutMillis");
    public static final Property<String> ZK_METADATA_PATH = Property.named("zk.metadata.path", "/segmentstore/containers", "zkMetadataPath");
    public static final Property<Integer> ZK_HIERARCHY_DEPTH = Property.named("zk.metadata.hierarchy.depth", 2, "zkHierarchyDepth");
    public static final Property<Integer> MAX_WRITE_ATTEMPTS = Property.named("write.attempts.count.max", 5, "maxWriteAttempts");
    public static final Property<Integer> BK_ENSEMBLE_SIZE = Property.named("ensemble.size", 3, "bkEnsembleSize");
    public static final Property<Integer> BK_ACK_QUORUM_SIZE = Property.named("ack.quorum.size", 2, "bkAckQuorumSize");
    public static final Property<Integer> BK_WRITE_QUORUM_SIZE = Property.named("write.quorum.size", 3, "bkWriteQuorumSize");
    public static final Property<Integer> BK_WRITE_TIMEOUT = Property.named("write.timeout.milliseconds", 60000, "bkWriteTimeoutMillis");
    public static final Property<Integer> BK_READ_TIMEOUT = Property.named("read.timeout.milliseconds", 30000, "bkReadTimeoutMillis");
    public static final Property<Integer> BK_USER_TCP_TIMEOUT = Property.named("user.tcp.timeout.milliseconds", 10000);
    public static final Property<Integer> BK_READ_BATCH_SIZE = Property.named("read.batch.size", 64, "readBatchSize");
    public static final Property<Integer> MAX_OUTSTANDING_BYTES = Property.named("write.outstanding.bytes.max", 256 * 1024 * 1024, "maxOutstandingBytes");
    public static final Property<Integer> BK_LEDGER_MAX_SIZE = Property.named("ledger.size.max", 1024 * 1024 * 1024, "bkLedgerMaxSize");
    public static final Property<String> BK_PASSWORD = Property.named("connect.security.auth.pwd", "", "bkPass");
    public static final Property<String> BK_LEDGER_PATH = Property.named("ledger.path", "", "bkLedgerPath");
    public static final Property<Boolean> BK_TLS_ENABLED = Property.named("connect.security.tls.enable", false, "tlsEnabled");
    public static final Property<String> TLS_TRUST_STORE_PATH = Property.named("connect.security.tls.trustStore.location", "config/client.truststore.jks", "tlsTrustStorePath");
    public static final Property<String> TLS_TRUST_STORE_PASSWORD_PATH = Property.named("connect.security.trustStore.pwd.location", "", "tlsTrustStorePasswordPath");
    public static final Property<Boolean> BK_ENFORCE_MIN_NUM_RACKS_PER_WRITE = Property.named("write.quorum.racks.minimumCount.enable", false, "enforceMinNumRacksPerWriteQuorum");
    public static final Property<Integer> BK_MIN_NUM_RACKS_PER_WRITE_QUORUM = Property.named("write.quorum.racks.count.min", 2, "minNumRacksPerWriteQuorum");
    public static final Property<String> BK_NETWORK_TOPOLOGY_SCRIPT_FILE_NAME = Property.named("networkTopology.script.location",
            "/opt/pravega/scripts/sample-bookkeeper-topology.sh", "networkTopologyScriptFileName");
    public static final Property<String> BK_DIGEST_TYPE = Property.named("digest.type.name", DigestType.CRC32C.name(), "digestType");

    public static final String COMPONENT_CODE = "bookkeeper";
    /**
     * Maximum append length, as specified by BookKeeper (this is hardcoded inside BookKeeper's code).
     */
    static final int MAX_APPEND_LENGTH = 1024 * 1024 - 1024;

    //endregion

    //region Members

    /**
     * The address (host and port) where the ZooKeeper controlling BookKeeper for this cluster can be found at.
     */
    @Getter
    private final String zkAddress;

    /**
     * Session Timeout for ZooKeeper.
     */
    @Getter
    private final Duration zkSessionTimeout;

    /**
     * Connection Timeout for ZooKeeper.
     */
    @Getter
    private final Duration zkConnectionTimeout;

    /**
     * Sub-namespace to use for ZooKeeper LogMetadata.
     */
    @Getter
    private final String zkMetadataPath;

    /**
     * Depth of the node hierarchy in ZooKeeper. 0 means flat, N means N deep, where each level is indexed by its
     * respective log id digit.
     */
    @Getter
    private final int zkHierarchyDepth;

    /**
     * The maximum number of times to attempt a write.
     */
    @Getter
    private final int maxWriteAttempts;

    /**
     * The path in ZooKeeper for the BookKeeper Ledger.
     */
    @Getter
    private final String bkLedgerPath;

    /**
     * The Ensemble Size for each Ledger created in BookKeeper.
     */
    @Getter
    private final int bkEnsembleSize;

    /**
     * The Ack Quorum Size for each Ledger created in BookKeeper.
     */
    @Getter
    private final int bkAckQuorumSize;

    /**
     * The Write Quorum Size for each Ledger created in BookKeeper.
     */
    @Getter
    private final int bkWriteQuorumSize;

    /**
     * The Write Timeout (BookKeeper client), in milliseconds.
     */
    @Getter
    private final int bkWriteTimeoutMillis;

    /**
     * The Read Timeout (BookKeeper client), in milliseconds.
     */
    @Getter
    private final int bkReadTimeoutMillis;

    /**
     * The timeout for non-responsive TCP connections, in milliseconds.
     */
    @Getter
    private final int bkUserTcpTimeoutMillis;

    /**
     * The number of Ledger Entries to read at once from BookKeeper.
     */
    @Getter
    private final int bkReadBatchSize;

    /**
     * The maximum number of bytes that can be outstanding per BookKeeperLog at any given time. This value should be used
     * for throttling purposes.
     */
    @Getter
    private final int maxOutstandingBytes;

    /**
     * The Maximum size of a ledger, in bytes. On or around this value the current ledger is closed and a new one
     * is created. By design, this property cannot be larger than Int.MAX_VALUE, since we want Ledger Entry Ids to be
     * representable with an Int.
     */
    @Getter
    private final int bkLedgerMaxSize;
    private final byte[] bkPassword;

    @Getter
    private final boolean isTLSEnabled;

    @Getter
    private final String tlsTrustStore;

    @Getter
    private final String tlsTrustStorePasswordPath;

    @Getter
    private final boolean enforceMinNumRacksPerWriteQuorum;

    @Getter
    private final int minNumRacksPerWriteQuorum;

    @Getter
    private final String networkTopologyFileName;

    @Getter
    private final DigestType digestType;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeperConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private BookKeeperConfig(TypedProperties properties) throws ConfigurationException {
        this.zkAddress = properties.get(ZK_ADDRESS).replace(",", ";");
        this.zkSessionTimeout = Duration.ofMillis(properties.getInt(ZK_SESSION_TIMEOUT));
        this.zkConnectionTimeout = Duration.ofMillis(properties.getInt(ZK_CONNECTION_TIMEOUT));
        this.zkMetadataPath = properties.get(ZK_METADATA_PATH);
        this.zkHierarchyDepth = properties.getInt(ZK_HIERARCHY_DEPTH);
        if (this.zkHierarchyDepth < 0) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be a non-negative integer.",
                    ZK_HIERARCHY_DEPTH, this.zkHierarchyDepth));
        }

        this.maxWriteAttempts = properties.getInt(MAX_WRITE_ATTEMPTS);
        this.bkLedgerPath = properties.get(BK_LEDGER_PATH);
        this.bkEnsembleSize = properties.getInt(BK_ENSEMBLE_SIZE);
        this.bkAckQuorumSize = properties.getInt(BK_ACK_QUORUM_SIZE);
        this.bkWriteQuorumSize = properties.getInt(BK_WRITE_QUORUM_SIZE);
        if (this.bkWriteQuorumSize < this.bkAckQuorumSize) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be greater than or equal to %s (%d).",
                    BK_WRITE_QUORUM_SIZE, this.bkWriteQuorumSize, BK_ACK_QUORUM_SIZE, this.bkAckQuorumSize));
        }

        this.bkWriteTimeoutMillis = properties.getInt(BK_WRITE_TIMEOUT);
        this.bkReadTimeoutMillis = properties.getInt(BK_READ_TIMEOUT);
        this.bkUserTcpTimeoutMillis = properties.getInt(BK_USER_TCP_TIMEOUT);
        this.bkReadBatchSize = properties.getInt(BK_READ_BATCH_SIZE);
        if (this.bkReadBatchSize < 1) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be a positive integer.",
                    BK_READ_BATCH_SIZE, this.bkReadBatchSize));
        }

        this.maxOutstandingBytes = properties.getInt(MAX_OUTSTANDING_BYTES);
        this.bkLedgerMaxSize = properties.getInt(BK_LEDGER_MAX_SIZE);
        this.bkPassword = properties.get(BK_PASSWORD).getBytes(StandardCharsets.UTF_8);
        this.isTLSEnabled = properties.getBoolean(BK_TLS_ENABLED);
        this.tlsTrustStore = properties.get(TLS_TRUST_STORE_PATH);
        this.tlsTrustStorePasswordPath = properties.get(TLS_TRUST_STORE_PASSWORD_PATH);

        this.enforceMinNumRacksPerWriteQuorum = properties.getBoolean(BK_ENFORCE_MIN_NUM_RACKS_PER_WRITE);
        this.minNumRacksPerWriteQuorum = properties.getInt(BK_MIN_NUM_RACKS_PER_WRITE_QUORUM);
        this.networkTopologyFileName = properties.get(BK_NETWORK_TOPOLOGY_SCRIPT_FILE_NAME);

        this.digestType = getDigestType(properties.get(BK_DIGEST_TYPE));
    }

    /**
     * Gets a value representing the Password to use for the creation and access of each BK Ledger.
     */
    byte[] getBKPassword() {
        return Arrays.copyOf(this.bkPassword, this.bkPassword.length);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<BookKeeperConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, BookKeeperConfig::new);
    }

    static DigestType getDigestType(String digestType) {
        if (digestType.equals(DigestType.MAC.name())) {
            return DigestType.MAC;
        } else if (digestType.equals(DigestType.CRC32.name())) {
            return DigestType.CRC32;
        } else if (digestType.equals(DigestType.DUMMY.name())) {
            return DigestType.DUMMY;
        } else {
            // Default digest for performance reasons.
            return DigestType.CRC32C;
        }
    }

    //endregion
}
