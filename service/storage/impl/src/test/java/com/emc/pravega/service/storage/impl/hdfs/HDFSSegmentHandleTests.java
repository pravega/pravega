/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.testcommon.AssertExtensions;
import java.util.ArrayList;
import java.util.List;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Unit tests for HDFSSegmentHandle.
 */
public class HDFSSegmentHandleTests {
    /**
     * Tests the ability to replace files in a handle.
     */
    @Test
    public void testReplaceFiles() {
        val handle = HDFSSegmentHandle.write("foo", createFiles(0, 10));
        val newFiles = createFiles(11, 11);
        handle.replaceFiles(new ArrayList<>(newFiles));
        AssertExtensions.assertListEquals("Unexpected result from getFiles after call to replaceFiles.", newFiles, handle.getFiles(), Object::equals);
    }

    /**
     * Tests the ability to remove the last file in the sequence in a handle.
     */
    @Test
    public void testRemoveLastFile() {
        val expectedFiles = createFiles(0, 10);
        val handle = HDFSSegmentHandle.write("foo", new ArrayList<>(expectedFiles));
        while (expectedFiles.size() > 1) {
            handle.removeLastFile();
            expectedFiles.remove(expectedFiles.size() - 1);
            AssertExtensions.assertListEquals("Unexpected result from getFiles after removing " + (10 - expectedFiles.size()) + ".",
                    expectedFiles, handle.getFiles(), Object::equals);
        }

        AssertExtensions.assertThrows(
                "removeLastFile did not fail when less than 2 handles.",
                handle::removeLastFile,
                ex -> ex instanceof IllegalStateException);
        AssertExtensions.assertListEquals("Unexpected result from getFiles after failed attempt to remove last files.",
                expectedFiles, handle.getFiles(), Object::equals);
    }

    /**
     * Tests the replaceLastFile method.
     */
    @Test
    public void testReplaceLastFile() {
        val expectedFiles = createFiles(0, 10);
        val lastFile = expectedFiles.get(expectedFiles.size() - 1);
        val validReplacement = new FileDescriptor(lastFile.getPath(), lastFile.getOffset(), lastFile.getLength() + 1, lastFile.getEpoch() + 1, true);
        val handle = HDFSSegmentHandle.write("foo", new ArrayList<>(expectedFiles));

        AssertExtensions.assertThrows(
                "removeLastFile did not fail when incorrect offset.",
                () -> handle.replaceLastFile(new FileDescriptor(lastFile.getPath(), lastFile.getOffset() + 1, lastFile.getLength(), lastFile.getEpoch(), true)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "removeLastFile did not fail when incorrect epoch.",
                () -> handle.replaceLastFile(new FileDescriptor(lastFile.getPath(), lastFile.getOffset(), lastFile.getLength(), lastFile.getEpoch() - 1, true)),
                ex -> ex instanceof IllegalArgumentException);

        handle.replaceLastFile(validReplacement);
        expectedFiles.set(expectedFiles.size() - 1, validReplacement);
        AssertExtensions.assertListEquals("Unexpected result from getFiles after replacing last file.",
                expectedFiles, handle.getFiles(), Object::equals);
    }

    private List<FileDescriptor> createFiles(int start, int count) {
        val result = new ArrayList<FileDescriptor>();
        for (int i = start; i < start + count; i++) {
            result.add(new FileDescriptor(new Path("foo" + i), i, 1, i, true));
        }

        return result;
    }
}
