/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

/**
 * Class that defines various status flags for {@link StorageMetadata}.
 */
public final class StatusFlags {
    /**
     * Not Initialized.
     */
    public static final int NONE = 0x0000;
    /**
     * Flag to indicate whether the chunk or segment is active or not.
     */
    public static final int ACTIVE = 0x0001;

    /**
     * Flag to indicate whether the  chunk or segment is sealed or not.
     */
    public static final int SEALED = 0x0002;

    /**
     * Flag to indicate whether the segment is storage system segment.
     */
    public static final int SYSTEM_SEGMENT = 0x0010;

    /**
     * Flag to indicate whether followup actions (like adding new chunks) after ownership changes are needed or not.
     */
    public static final int OWNERSHIP_CHANGED = 0x0008;
}
