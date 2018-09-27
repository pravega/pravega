/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import static org.junit.Assert.assertEquals;

public class RecordsHelperTest {
    @Test
    public void historyTimeIndexTest() {
        List<Long> leaves = Lists.newArrayList(10L, 30L, 75L, 100L, 152L);
        HistoryTimeIndexRootNode root = new HistoryTimeIndexRootNode(leaves);
        HistoryTimeIndexLeaf leaf0 = new HistoryTimeIndexLeaf(Lists.newArrayList(10L, 11L, 18L, 25L, 29L));
        HistoryTimeIndexLeaf leaf1 = new HistoryTimeIndexLeaf(Lists.newArrayList(30L, 32L, 35L, 45L, 71L));
        HistoryTimeIndexLeaf leaf2 = new HistoryTimeIndexLeaf(Lists.newArrayList(75L, 81L, 94L, 96L, 99L));
        HistoryTimeIndexLeaf leaf3 = new HistoryTimeIndexLeaf(Lists.newArrayList(100L, 132L, 135L, 145L, 151L));
        HistoryTimeIndexLeaf leaf4 = new HistoryTimeIndexLeaf(Lists.newArrayList(152L, 312L, 351L, 415L, 711L));
        List<HistoryTimeIndexLeaf> leavesRecords = Lists.newArrayList(leaf0, leaf1, leaf2, leaf3, leaf4);
        int leaf = root.findLeafNode(0L);
        assertEquals(leaf, 0);
        leaf = root.findLeafNode(10L);
        assertEquals(leaf, 0);
        leaf = root.findLeafNode(77L);
        assertEquals(leaf, 2);
        leaf = root.findLeafNode(166L);
        assertEquals(leaf, 4);

        leaf = root.findLeafNode(0L);
        int recordIndex = leavesRecords.get(leaf).findIndexAtTime(0L);
        assertEquals(recordIndex, 0);
        recordIndex = leavesRecords.get(leaf).findIndexAtTime(10L);
        assertEquals(recordIndex, 0);
        recordIndex = leavesRecords.get(leaf).findIndexAtTime(21L);
        assertEquals(recordIndex, 2);
        recordIndex = leavesRecords.get(leaf).findIndexAtTime(29L);
        assertEquals(recordIndex, 4);
    }

    @Test
    public void binarySearchTest() {
        List<Long> list = Lists.newArrayList(10L, 30L, 75L, 100L, 152L, 200L, 400L, 700L);

        int index = RecordHelper.binarySearch(list, 0L, x -> x);
        assertEquals(index, 0);
        index = RecordHelper.binarySearch(list, 29L, x -> x);
        assertEquals(index, 0);
        index = RecordHelper.binarySearch(list, 101L, x -> x);
        assertEquals(index, 3);
        index = RecordHelper.binarySearch(list, Integer.MAX_VALUE, x -> x);
        assertEquals(index, 7);
    }
}
