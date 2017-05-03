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
package io.pravega.service.storage.impl.hdfs;

import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the ExistsOperation class.
 */
public class ExistsOperationTests extends FileSystemOperationTestBase {
    // We introduce the separator into the name here, to make sure we can still extract the values correctly from there.
    private static final String SEGMENT_NAME = "segment"+FileSystemOperation.PART_SEPARATOR+"segment";

    /**
     * Tests the ExistsOperation in various scenarios.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testExists() throws Exception {
        final int epoch = 1;
        final int offset = 0;
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(epoch, fs);

        // Not exists.
        Assert.assertFalse("Unexpected result for missing segment (no files).", new ExistsOperation(SEGMENT_NAME, context).call());

        // Some other segment exists.
        context.createEmptyFile(SEGMENT_NAME + "foo", offset);
        Assert.assertFalse("Unexpected result for missing segment (no files, but other segment exists).",
                new ExistsOperation(SEGMENT_NAME, context).call());

        // Malformed name (missing parts).
        final String correctFileName = context.getFileName(SEGMENT_NAME, offset).toString();
        fs.clear();
        fs.createNewFile(new Path(correctFileName.substring(0, correctFileName.indexOf(FileSystemOperation.PART_SEPARATOR))));
        Assert.assertFalse("Unexpected result for missing segment (malformed name 1).", new ExistsOperation(SEGMENT_NAME, context).call());
        fs.clear();
        fs.createNewFile(new Path(correctFileName.substring(0, correctFileName.lastIndexOf(FileSystemOperation.PART_SEPARATOR))));
        Assert.assertFalse("Unexpected result for missing segment (malformed name 2).", new ExistsOperation(SEGMENT_NAME, context).call());

        // Malformed name (non-numeric parts for offset and/or epoch).
        fs.clear();
        fs.createNewFile(new Path(correctFileName.replaceAll(Integer.toString(epoch), "")));
        Assert.assertFalse("Unexpected result for missing segment (missing epoch).", new ExistsOperation(SEGMENT_NAME, context).call());
        fs.clear();
        fs.createNewFile(new Path(correctFileName.replaceAll(Integer.toString(epoch), "A")));
        Assert.assertFalse("Unexpected result for missing segment (invalid epoch).", new ExistsOperation(SEGMENT_NAME, context).call());
        fs.clear();
        fs.createNewFile(new Path(correctFileName.replaceAll(Integer.toString(offset), "")));
        Assert.assertFalse("Unexpected result for missing segment (missing offset).", new ExistsOperation(SEGMENT_NAME, context).call());
        fs.clear();
        fs.createNewFile(new Path(correctFileName.replaceAll(Integer.toString(offset), "B")));
        Assert.assertFalse("Unexpected result for missing segment (invalid offset).", new ExistsOperation(SEGMENT_NAME, context).call());

        // Exists.
        fs.clear();
        fs.createNewFile(new Path(correctFileName));
        Assert.assertTrue("Unexpected result for existing segment.", new ExistsOperation(SEGMENT_NAME, context).call());

        // Exists but corrupted (i.e., missing first file)
        fs.clear();
        context.createEmptyFile(SEGMENT_NAME, 1);
        AssertExtensions.assertThrows(
                "Exists did not fail when segment with corrupted files was encountered.",
                new ExistsOperation(SEGMENT_NAME, context)::call,
                ex -> ex instanceof SegmentFilesCorruptedException);
    }
}
