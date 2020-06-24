/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
<<<<<<< HEAD
<<<<<<< HEAD
import io.pravega.common.util.BufferView;
=======
import io.pravega.common.util.ArrayView;
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
import io.pravega.common.util.BufferView;
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import lombok.RequiredArgsConstructor;
<<<<<<< HEAD
<<<<<<< HEAD
import lombok.SneakyThrows;
=======
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
import lombok.SneakyThrows;
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)

/**
 * Translates Table Segment Keys from an external form into an internal one and back.
 */
abstract class KeyTranslator {
    /**
     * Gets a {@link KeyTranslator} that does not make any changes.
     *
     * @return The identity translator.
     */
    static KeyTranslator identity() {
        return new IdentityTranslator();
    }

    /**
     * Creates a {@link KeyTranslator} that assigns a 1-byte partition to each key.
     *
     * @param partition The partition.
     * @return The {@link KeyTranslator}.
     */
    static KeyTranslator partitioned(byte partition) {
        return new PartitionKeyTranslator(partition);
    }

    //region Operations

    /**
     * Translates the given external Key data into an internal form.
     *
<<<<<<< HEAD
<<<<<<< HEAD
     * @param external The external Key data. This {@link BufferView} instance will not be altered.
     * @return A new {@link BufferView} representing the internal Key data.
     */
    abstract BufferView inbound(BufferView external);
=======
     * @param external The external Key data. This {@link ArrayView} instance will not be altered.
     * @return A new {@link ArrayView} representing the internal Key data.
     */
    abstract ArrayView inbound(ArrayView external);
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
     * @param external The external Key data. This {@link BufferView} instance will not be altered.
     * @return A new {@link BufferView} representing the internal Key data.
     */
    abstract BufferView inbound(BufferView external);
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)

    /**
     * Translates the given external {@link TableKey} data into an internal form.
     *
     * @param external The external {@link TableKey}. This {@link TableKey} instance will not be altered.
     * @return A new {@link TableKey} representing the internal Key data. This will have the same version as the external
     * one.
     */
    TableKey inbound(TableKey external) {
        return TableKey.versioned(inbound(external.getKey()), external.getVersion());
    }

    /**
     * Translates the given external {@link TableEntry} into an internal form.
     *
     * @param external The external {@link TableEntry}. This {@link TableEntry} instance will not be altered.
     * @return A new {@link TableEntry} with the altered Key. The Key Version and Value are not modified.
     */
    TableEntry inbound(TableEntry external) {
        return TableEntry.versioned(inbound(external.getKey().getKey()), external.getValue(), external.getKey().getVersion());
    }

    /**
     * Translates the given internal Key data into an external form.
     *
<<<<<<< HEAD
<<<<<<< HEAD
     * @param internal The internal Key data. This {@link BufferView} instance will not be altered.
     * @return A new {@link BufferView} representing the external Key data.
     */
    abstract BufferView outbound(BufferView internal);
=======
     * @param internal The internal Key data. This {@link ArrayView} instance will not be altered.
     * @return A new {@link ArrayView} representing the external Key data.
     */
    abstract ArrayView outbound(ArrayView internal);
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
     * @param internal The internal Key data. This {@link BufferView} instance will not be altered.
     * @return A new {@link BufferView} representing the external Key data.
     */
    abstract BufferView outbound(BufferView internal);
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)

    /**
     * Translates the given internal {@link TableKey} data into an external form.
     *
     * @param internal The internal {@link TableKey}. This {@link TableKey} instance will not be altered.
     * @return A new {@link TableKey} representing the external Key data. This will have the same version as the internal
     * one.
     */
    TableKey outbound(TableKey internal) {
        return TableKey.versioned(outbound(internal.getKey()), internal.getVersion());
    }

    /**
     * Translates the given internal {@link TableEntry} into an external form.
     *
     * @param internal The internal {@link TableEntry}. This {@link TableEntry} instance will not be altered.
     * @return A new {@link TableEntry} with the altered Key. The Key Version and Value are not modified.
     */
    TableEntry outbound(TableEntry internal) {
        return internal == null
                ? null
                : TableEntry.versioned(outbound(internal.getKey().getKey()), internal.getValue(), internal.getKey().getVersion());
    }

    /**
<<<<<<< HEAD
<<<<<<< HEAD
     * Determines whether the given {@link BufferView} represents a key that has been modified.
=======
     * Determines whether the given {@link ArrayView} represents a key that has been modified.
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
     * Determines whether the given {@link BufferView} represents a key that has been modified.
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
     *
     * @param key The key to check.
     * @return True if this is the result of a call to {@link #inbound}, false otherwise.
     */
<<<<<<< HEAD
<<<<<<< HEAD
    abstract boolean isInternal(BufferView key);
=======
    abstract boolean isInternal(ArrayView key);
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
    abstract boolean isInternal(BufferView key);
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)

    /**
     * Determines whether the given {@link TableKey} represents a key that has been modified.
     *
     * @param key The key to check.
     * @return True if key is the result of a call to {@link #inbound}, false otherwise.
     */
    boolean isInternal(TableKey key) {
        return isInternal(key.getKey());
    }

    //endregion

    //region PartitionKeyTranslator

    @RequiredArgsConstructor
    private static class PartitionKeyTranslator extends KeyTranslator {
        private final byte partition;

        @Override
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
        @SneakyThrows
        BufferView inbound(BufferView external) {
            return BufferView.builder(2)
                    .add(new ByteArraySegment(new byte[]{this.partition}))
                    .add(external)
                    .build();
<<<<<<< HEAD
        }

        @Override
        BufferView outbound(BufferView internal) {
            Preconditions.checkArgument(internal.getLength() >= 1,
                    "Key too short. Expected at least 1, given %s.", internal.getLength());
            BufferView.Reader reader = internal.getBufferViewReader();
            byte p = reader.readByte();
            Preconditions.checkArgument(p == this.partition, "Wrong partition. Expected %s, found %s.", this.partition, p);
            if (reader.available() == 0) {
                // There was no key to begin with.
                return BufferView.empty();
            }

            return reader.readSlice(reader.available());
        }

        @Override
        boolean isInternal(BufferView key) {
            if (key.getLength() < 1) {
                return false;
            }
            return key.getBufferViewReader().readByte() == this.partition;
=======
        ArrayView inbound(ArrayView external) {
            byte[] data = new byte[1 + external.getLength()];
            data[0] = this.partition;
            if (external.getLength() > 0) {
                external.copyTo(data, 1, external.getLength());
            }

            return new ByteArraySegment(data);
=======
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
        }

        @Override
        BufferView outbound(BufferView internal) {
            Preconditions.checkArgument(internal.getLength() >= 1,
                    "Key too short. Expected at least 1, given %s.", internal.getLength());
            BufferView.Reader reader = internal.getBufferViewReader();
            byte p = reader.readByte();
            Preconditions.checkArgument(p == this.partition, "Wrong partition. Expected %s, found %s.", this.partition, p);
            if (reader.available() == 0) {
                // There was no key to begin with.
                return BufferView.empty();
            }

            return reader.readSlice(reader.available());
        }

        @Override
        boolean isInternal(BufferView key) {
            if (key.getLength() < 1) {
                return false;
            }
<<<<<<< HEAD
            return key.get(0) == this.partition;
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
            return key.getBufferViewReader().readByte() == this.partition;
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
        }
    }

    //endregion

    //region IdentityTranslator

    private static class IdentityTranslator extends KeyTranslator {
        @Override
<<<<<<< HEAD
<<<<<<< HEAD
        BufferView inbound(BufferView external) {
=======
        ArrayView inbound(ArrayView external) {
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
        BufferView inbound(BufferView external) {
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
            return external;
        }

        @Override
        TableKey inbound(TableKey external) {
            return external;
        }

        @Override
        TableEntry inbound(TableEntry external) {
            return external;
        }

        @Override
<<<<<<< HEAD
<<<<<<< HEAD
        BufferView outbound(BufferView internal) {
=======
        ArrayView outbound(ArrayView internal) {
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
        BufferView outbound(BufferView internal) {
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
            return internal;
        }

        @Override
        TableKey outbound(TableKey internal) {
            return internal;
        }

        @Override
        TableEntry outbound(TableEntry internal) {
            return internal;
        }

        @Override
<<<<<<< HEAD
<<<<<<< HEAD
        boolean isInternal(BufferView key) {
=======
        boolean isInternal(ArrayView key) {
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
        boolean isInternal(BufferView key) {
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
            return true;
        }
    }

    //endregion

}
