/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.collect;

import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.val;

/**
 * Layout: Header|Data|Footer
 * * Header: Version(1)|Flags(1)|Id(4)|Count(4)
 * * Data: List{Key(KL)|Value(VL)}
 * * Footer: Id(4)
 */
abstract class BTreePageLayout {
    //region Members

    protected final BTreePage.Config pageConfig;

    //endregion

    //region Constructor

    BTreePageLayout(int keyLength, int valueLength, int maxPageSize) {
        this.pageConfig = new BTreePage.Config(keyLength, valueLength, maxPageSize, isIndexLayout());
    }

    //endregion

    BTreePage parse(ByteArraySegment pageContents) {
        return new BTreePage(this.pageConfig, pageContents);
    }

    int getMaxPageSize() {
        return this.pageConfig.getMaxPageSize();
    }

    BTreePage createEmptyRoot() {
        return new BTreePage(this.pageConfig);
    }

    protected abstract boolean isIndexLayout();

    //region Index Layout

    static class Index extends BTreePageLayout {
        private static final int VALUE_LENGTH = Long.BYTES + Integer.BYTES;

        Index(int keyLength, int maxPageSize) {
            super(keyLength, VALUE_LENGTH, maxPageSize);
        }

        BTreePagePointer getPagePointer(byte[] key, BTreePage page) {
            val sr = page.search(key);
            assert sr.getPosition() >= 0;

            ByteArraySegment ptr = page.getValueAt(sr.getPosition());
            long pageOffset = BitConverter.readLong(ptr, 0);
            int pageLength = BitConverter.readInt(ptr, Long.BYTES);
            return new BTreePagePointer(page.getKeyAt(sr.getPosition()), pageOffset, pageLength);
        }

        ByteArraySegment serializePointer(BTreePagePointer pointer) {
            ByteArraySegment result = new ByteArraySegment(new byte[Long.BYTES + Integer.BYTES]);
            BitConverter.writeLong(result, 0, pointer.getOffset());
            BitConverter.writeInt(result, Long.BYTES, pointer.getLength());
            return result;
        }

        void updatePointers(Collection<BTreePagePointer> pointers, BTreePage page) {
            val toUpdate = pointers.stream()
                                   .map(p -> new ArrayTuple(p.getKey(), serializePointer(p)))
                                   .collect(Collectors.toList());
            page.update(toUpdate);
        }

        @Override
        protected boolean isIndexLayout() {
            return true;
        }
    }

    //endregion

    //region Leaf Layout

    static class Leaf extends BTreePageLayout {
        Leaf(int keyLength, int valueLength, int maxPageSize) {
            super(keyLength, valueLength, maxPageSize);
        }

        @Override
        protected boolean isIndexLayout() {
            return false;
        }

        ByteArraySegment getValue(byte[] key, BTreePage page) {
            val sr = page.search(key);
            if (!sr.isExactMatch()) {
                // Nothing found
                return null;
            }

            return page.getValueAt(sr.getPosition());
        }
    }

    //endregion
}

