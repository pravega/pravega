/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.util;

import com.google.common.base.Preconditions;

/**
 * Utilities for naming and validating pravega objects - streams, scopes, etc.
 */
public class NameUtils {

    // The prefix which will be used to name all internal streams.
    public static final String INTERNAL_STREAM_NAME_PREFIX = "_";

    /**
     * @param streamName    The stream name for which we need to construct an internal name.
     * @return              The stream name which has to be used internally in the pravega system.
     */
    public static String getInternalNameForStream(String streamName) {
        return INTERNAL_STREAM_NAME_PREFIX + streamName;
    }

    /**
     * Validates a stream name.
     *
     * @param name User supplied stream name to validate.
     * @return The name in the case is valid.
     */
    public static String validateUserStreamName(String name) {
        Preconditions.checkNotNull(name);
        Preconditions.checkArgument(name.matches("[a-zA-Z0-9]+"), "Name must be [a-zA-Z0-9]+");
        return name;
    }

    /**
     * Validates an internal stream name.
     *
     * @param name Stream name to validate.
     * @return The name in the case is valid.
     */
    public static String validateStreamName(String name) {
        Preconditions.checkNotNull(name);

        // In addition to user stream names, pravega internally created stream have a special prefix.
        final String matcher = "[" + INTERNAL_STREAM_NAME_PREFIX + "]?[a-zA-Z0-9]+";
        Preconditions.checkArgument(name.matches(matcher), "Name must be " + matcher);
        return name;
    }

    /**
     * Validates a scope name.
     *
     * @param name Scope name to validate.
     * @return The name in the case is valid.
     */
    public static String validateScopeName(String name) {
        return validateUserStreamName(name);
    }

    /**
     * Validates a reader group name.
     *
     * @param name Reader group name to validate.
     * @return The name in the case is valid.
     */
    public static String validateReaderGroupName(String name) {
        return validateUserStreamName(name);
    }
}
