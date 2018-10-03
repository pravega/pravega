/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import io.pravega.common.util.ArrayView;
import java.util.Collection;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Getter;

/**
 * Defines a Listener for Key Updates.
 */
@Builder
@Getter
public class UpdateListener {
    /**
     * The Segment this UpdateListener is associated with.
     */
    private String segmentName;
    /**
     * The Keys to filter. If null or empty, will receive updates on ALL keys.
     */
    private final Collection<ArrayView> keys; // TODO: ArrayView must implement equals & hashCode.
    /**
     * Invoked every time a Key associated with this UpdateListener is inserted or updated into this TableSegment.
     */
    private final Consumer<TableEntry> updateEntryCallback;
    /**
     * Invoked every time a Key associated with this UpdateListener is removed from this TableSegment.
     */
    private final Consumer<TableKey> removeKeyCallback;
    /**
     * Invoked when the TableSegment that this UpdateListener is associated with is sealed.
     */
    private final Runnable sealedCallback;
    /**
     * Invoked when the TableSegment that this UpdateListener is associated with is deleted.
     */
    private final Runnable deletedCallback;
    /**
     * Invoked when the TableSegment that this UpdateListener is associated with is merged (into another TableSegment).
     */
    private final Runnable mergedCallback;

    //endregion
}