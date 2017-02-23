/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.CheckpointStoreException;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.PositionImpl;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Checkpoint store test.
 */
public abstract class CheckpointStoreTests {

    protected CheckpointStore checkpointStore;

    @Before
    public abstract void setupCheckpointStore() throws Exception;

    @After
    public abstract void cleanupCheckpointStore() throws IOException;

    @Test
    public void folderOperationTests() throws CheckpointStoreException {

        final String process1 = "process1";
        final String readerGroup1 = "rg1";
        final String readerGroup2 = "rg2";
        final String reader1 = "reader1";
        final String reader2 = "reader2";

        checkpointStore.addReaderGroup(process1, readerGroup1);
        List<String> result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(readerGroup1, result.get(0));

        checkpointStore.addReader(process1, readerGroup1, reader1);
        Map<String, Position> resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        Assert.assertNull(resultMap.get(reader1));

        Position position = new PositionImpl(Collections.emptyMap());
        checkpointStore.setPosition(process1, readerGroup1, reader1, position);
        resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        Assert.assertNotNull(resultMap.get(reader1));
        Assert.assertEquals(position, resultMap.get(reader1));

        try {
            checkpointStore.setPosition(process1, readerGroup1, "randomReader", position);
            Assert.assertTrue(false);
        } catch (CheckpointStoreException cse) {
            Assert.assertEquals(CheckpointStoreException.Type.NoNode, cse.getType());
        }

        try {
            checkpointStore.removeReaderGroup(process1, readerGroup1);
            Assert.assertFalse(true);
        } catch (CheckpointStoreException cse) {
            Assert.assertEquals(CheckpointStoreException.Type.Active, cse.getType());
        }

        result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(readerGroup1, result.get(0));

        checkpointStore.addReader(process1, readerGroup1, reader2);
        resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(2, resultMap.size());
        Assert.assertNotNull(resultMap.get(reader1));
        Assert.assertNull(resultMap.get(reader2));

        checkpointStore.addReaderGroup(process1, readerGroup2);
        result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());

        Map<String, Position> map = checkpointStore.sealReaderGroup(process1, readerGroup2);
        Assert.assertNotNull(map);
        Assert.assertEquals(0, map.size());

        checkpointStore.removeReaderGroup(process1, readerGroup2);
        result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(readerGroup1, result.get(0));

        checkpointStore.removeReader(process1, readerGroup1, reader1);

        checkpointStore.removeReader(process1, readerGroup1, "randomReader");

        resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        Assert.assertNull(resultMap.get(reader2));

        checkpointStore.removeReader(process1, readerGroup1, reader2);
        resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(0, resultMap.size());

        try {
            checkpointStore.addReaderGroup(process1, readerGroup1);
            Assert.assertTrue(false);
        } catch (CheckpointStoreException cse) {
            Assert.assertEquals(CheckpointStoreException.Type.NodeExists, cse.getType());
        }

        result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(readerGroup1, result.get(0));

        map = checkpointStore.sealReaderGroup(process1, readerGroup1);
        Assert.assertNotNull(map);
        Assert.assertEquals(0, map.size());

        checkpointStore.removeReaderGroup(process1, readerGroup1);
        result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.size());
    }
}
