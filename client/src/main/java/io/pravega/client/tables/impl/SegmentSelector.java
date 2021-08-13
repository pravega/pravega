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
package io.pravega.client.tables.impl;

import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.NonNull;

/**
 * Selector for Table Segments.
 */
class SegmentSelector implements AutoCloseable {
    //region Members

    @Getter
    private final KeyValueTableInfo kvt;
    private final TableSegmentFactory tableSegmentFactory;
    private final KeyValueTableSegments segmentsByRange;
    @GuardedBy("segments")
    private final Map<Segment, TableSegment> segments = new HashMap<>();
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link SegmentSelector} class.
     *
     * @param kvt                 The {@link KeyValueTableInfo} describing the Key Value Table.
     * @param controller          The {@link Controller} to use.
     * @param tableSegmentFactory A {@link TableSegmentFactory} to create {@link TableSegment} instances.
     */
    SegmentSelector(@NonNull KeyValueTableInfo kvt, @NonNull Controller controller, @NonNull TableSegmentFactory tableSegmentFactory) {
        this.kvt = kvt;
        this.tableSegmentFactory = tableSegmentFactory;
        this.segmentsByRange = initializeSegments(kvt, controller);
        assert this.segmentsByRange != null;
        this.closed = new AtomicBoolean(false);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            ArrayList<TableSegment> toClose;
            synchronized (this.segments) {
                toClose = new ArrayList<>(this.segments.values());
                this.segments.clear();
            }

            toClose.forEach(TableSegment::close);
        }
    }

    //endregion

    //region Operations

    /**
     * Gets the {@link TableSegment} that maps the given Key.
     *
     * @param key The Key to query.
     * @return The {@link TableSegment} that maps to the given key.
     */
    TableSegment getTableSegment(@NonNull ByteBuffer key) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Segment s = this.segmentsByRange.getSegmentForKey(key.duplicate());
        return getTableSegment(s);
    }

    private TableSegment getTableSegment(Segment s) {
        synchronized (this.segments) {
            TableSegment ts = this.segments.get(s);
            if (ts == null) {
                ts = this.tableSegmentFactory.forSegment(s);
                this.segments.put(s, ts);
            }

            return ts;
        }
    }

    /**
     * Gets a Collection of all the {@link TableSegment}s for the KeyValueTable this {@link SegmentSelector} manages.
     *
     * @return A Collection of all {@link TableSegment}s in the KeyValueTable.
     */
    Collection<TableSegment> getAllTableSegments() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentsByRange.getSegments().stream().map(this::getTableSegment).collect(Collectors.toList());
    }

    /**
     * Gets a value indicating the number of active Table Segments for this Key Value Table.
     *
     * @return The number of active Table Segments.
     */
    int getSegmentCount() {
        return this.segmentsByRange.getSegmentCount();
    }

    private KeyValueTableSegments initializeSegments(KeyValueTableInfo kvt, Controller controller) {
        return Futures.getAndHandleExceptions(
                controller.getCurrentSegmentsForKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName()),
                RuntimeException::new);
    }

    //endregion
}
