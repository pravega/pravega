/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * General configuration for BookKeeper Client.
 */
public class BookKeeperConfig {
    //region Config Names

    public static final Property<String> ZK_ADDRESS = Property.named("zkAddress", "localhost:2181");
    public static final Property<Integer> ZK_SESSION_TIMEOUT = Property.named("zkSessionTimeoutMillis", 10000);
    public static final Property<Integer> ZK_CONNECTION_TIMEOUT = Property.named("zkConnectionTimeoutMillis", 10000);
    public static final Property<String> ZK_NAMESPACE = Property.named("zkNamespace", "/pravega/containers");
    public static final Property<Retry.RetryWithBackoff> RETRY_POLICY = Property.named("retryPolicy", Retry.withExpBackoff(100, 4, 5, 30000));
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
     * The Retry Policy base to use for all BookKeeper parameters.
     */
    @Getter
    private Retry.RetryWithBackoff retryPolicy;

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
        this.retryPolicy = properties.getRetryWithBackoff(RETRY_POLICY);
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
