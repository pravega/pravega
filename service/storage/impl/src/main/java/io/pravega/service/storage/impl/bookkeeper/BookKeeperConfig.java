/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.bookkeeper;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.common.util.Property;
import io.pravega.common.util.Retry;
import io.pravega.common.util.TypedProperties;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import lombok.Getter;

/**
 * General configuration for BookKeeper Client.
 */
public class BookKeeperConfig {
    //region Config Names

    public static final Property<String> ZK_ADDRESS = Property.named("zkAddress", "localhost:2181");
    public static final Property<Integer> ZK_SESSION_TIMEOUT = Property.named("zkSessionTimeoutMillis", 10000);
    public static final Property<Integer> ZK_CONNECTION_TIMEOUT = Property.named("zkConnectionTimeoutMillis", 10000);
    public static final Property<String> ZK_NAMESPACE = Property.named("zkNamespace", "/pravega/segmentstore/containers");
    public static final Property<Integer> ZK_HIERARCHY_DEPTH = Property.named("zkHierarchyDepth", 2);
    public static final Property<Retry.RetryWithBackoff> RETRY_POLICY = Property.named("retryPolicy", Retry.withExpBackoff(100, 4, 5, 30000));
    public static final Property<Integer> BK_ENSEMBLE_SIZE = Property.named("bkEnsembleSize", 3);
    public static final Property<Integer> BK_ACK_QUORUM_SIZE = Property.named("bkAckQuorumSize", 3);
    public static final Property<Integer> BK_WRITE_QUORUM_SIZE = Property.named("bkWriteQuorumSize", 3);
    public static final Property<Integer> BK_LEDGER_MAX_SIZE = Property.named("bkLedgerMaxSize", 1024 * 1024 * 1024);
    public static final Property<String> BK_PASSWORD = Property.named("bkPass", "");
    private static final String COMPONENT_CODE = "bookkeeper";

    //endregion

    //region Members

    /**
     * The address (host and port) where BookKeeper is listening.
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
     * ZooKeeper root (namespace).
     */
    @Getter
    private final String namespace;

    /**
     * Depth of the node hierarchy in ZooKeeper. 0 means flat, N means N deep, where each level is indexed by its respective
     * log id digit.
     */
    @Getter
    private final int zkHierarchyDepth;

    /**
     * The Retry Policy base to use for all BookKeeper parameters.
     */
    @Getter
    private final Retry.RetryWithBackoff retryPolicy;

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
     * The Maximum size of a ledger, in bytes. On or around this value the current ledger is closed and a new one
     * is created. By design, this property cannot be larger than Int.MAX_VALUE, since we want Ledger Entry Ids to be
     * representable with an Int.
     */
    @Getter
    private final int bkLedgerMaxSize;
    private final byte[] bkPassword;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeperConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private BookKeeperConfig(TypedProperties properties) throws ConfigurationException {
        this.zkAddress = properties.get(ZK_ADDRESS);
        this.zkSessionTimeout = Duration.ofMillis(properties.getInt(ZK_SESSION_TIMEOUT));
        this.zkConnectionTimeout = Duration.ofMillis(properties.getInt(ZK_CONNECTION_TIMEOUT));
        this.namespace = properties.get(ZK_NAMESPACE);
        this.zkHierarchyDepth = properties.getInt(ZK_HIERARCHY_DEPTH);
        if (this.zkHierarchyDepth < 0) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be a non-negative integer.",
                    ZK_HIERARCHY_DEPTH, this.zkHierarchyDepth));
        }

        this.retryPolicy = properties.getRetryWithBackoff(RETRY_POLICY);
        this.bkEnsembleSize = properties.getInt(BK_ENSEMBLE_SIZE);
        this.bkAckQuorumSize = properties.getInt(BK_ACK_QUORUM_SIZE);
        this.bkWriteQuorumSize = properties.getInt(BK_WRITE_QUORUM_SIZE);
        if (this.bkWriteQuorumSize < this.bkAckQuorumSize) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be greater than or equal to %s (%d).",
                    BK_WRITE_QUORUM_SIZE, this.bkWriteQuorumSize, BK_ACK_QUORUM_SIZE, this.bkAckQuorumSize));
        }

        this.bkLedgerMaxSize = properties.getInt(BK_LEDGER_MAX_SIZE);
        this.bkPassword = properties.get(BK_PASSWORD).getBytes(Charset.forName("UTF-8"));
    }

    /**
     * Gets a value representing the Password to use for the creation and access of each BK Ledger.
     */
    public byte[] getBKPassword() {
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

    //endregion
}
