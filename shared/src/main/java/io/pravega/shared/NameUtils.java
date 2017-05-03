/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.shared;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Utilities for naming and validating pravega objects - streams, scopes, etc.
 */
public class NameUtils {

    // The prefix which will be used to name all internal streams.
    public static final String INTERNAL_NAME_PREFIX = "_";

    // The scope name which has to be used when creating internally used pravega streams.
    public static final String INTERNAL_SCOPE_NAME = "_system";

    // The prefix which has to be appended to streams created internally for readerGroups.
    @VisibleForTesting
    static final String READER_GROUP_STREAM_PREFIX = INTERNAL_NAME_PREFIX + "RG";

    /**
     * Construct an internal representation of stream name. This is required to distinguish between user created
     * and pravega internally created streams.
     *
     * @param streamName    The stream name for which we need to construct an internal name.
     * @return              The stream name which has to be used internally in the pravega system.
     */
    public static String getInternalNameForStream(String streamName) {
        return INTERNAL_NAME_PREFIX + streamName;
    }

    /**
     * Construct a stream name which will internally be used by the readergroup implementation.
     *
     * @param groupNameName The readergroup name for which we need to construct an internal stream name.
     * @return              The stream name which has to be used internally by the reader group implementation.
     */
    public static String getStreamForReaderGroup(String groupNameName) {
        return READER_GROUP_STREAM_PREFIX + groupNameName;
    }

    /**
     * Validates a user created stream name.
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
        final String matcher = "[" + INTERNAL_NAME_PREFIX + "]?[a-zA-Z0-9]+";
        Preconditions.checkArgument(name.matches(matcher), "Name must be " + matcher);
        return name;
    }

    /**
     * Validates a user created scope name.
     *
     * @param name Scope name to validate.
     * @return The name in the case is valid.
     */
    public static String validateUserScopeName(String name) {
        return validateUserStreamName(name);
    }

    /**
     * Validates a scope name.
     *
     * @param name Scope name to validate.
     * @return The name in the case is valid.
     */
    public static String validateScopeName(String name) {
        return validateStreamName(name);
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
