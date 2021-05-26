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
package io.pravega.segmentstore.storage.metadata;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

/**
 * Unit tests for {@link SegmentMetadata}.
 */
public class SegmentMetadataTests {

    @Test
    public void testCheckInvariant() {
        SegmentMetadata[] invalidDataList = new SegmentMetadata[] {
                SegmentMetadata.builder()
                        .name("Test")
                        .chunkCount(1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(-1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(0)
                        .startOffset(-1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(0)
                        .startOffset(0)
                        .firstChunkStartOffset(-1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(0)
                        .startOffset(0)
                        .firstChunkStartOffset(0)
                        .lastChunkStartOffset(-1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(1)
                        .firstChunk("chunk1")// Wrong
                        .lastChunk("chunk1") // Wrong
                        .startOffset(1)
                        .firstChunkStartOffset(1)
                        .lastChunkStartOffset(2) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1")// Wrong
                        .lastChunk("chunk2") // Wrong
                        .startOffset(0)
                        .firstChunkStartOffset(4) // Wrong
                        .lastChunkStartOffset(3) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1")// Wrong
                        .lastChunk("chunk2") // Wrong
                        .startOffset(1) // Wrong
                        .firstChunkStartOffset(2)
                        .lastChunkStartOffset(3)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(2) // Wrong
                        .firstChunk("chunk1")// Wrong
                        .lastChunk("chunk2") // Wrong
                        .startOffset(1)
                        .firstChunkStartOffset(0)
                        .lastChunkStartOffset(3) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .startOffset(1)
                        .firstChunkStartOffset(0)
                        .lastChunkStartOffset(3)
                        .chunkCount(-1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .startOffset(1) // Wrong
                        .firstChunkStartOffset(0)
                        .lastChunkStartOffset(0)
                        .chunkCount(1)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10) // Wrong
                        .startOffset(8)
                        .firstChunkStartOffset(8)
                        .lastChunkStartOffset(8)
                        .chunkCount(0) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10) // Wrong
                        .startOffset(8)
                        .firstChunkStartOffset(8)
                        .lastChunkStartOffset(8) // Wrong
                        .chunkCount(1)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1")// Wrong
                        .lastChunk("chunk2") // Wrong
                        .startOffset(8)
                        .firstChunkStartOffset(8)
                        .lastChunkStartOffset(8)
                        .chunkCount(1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1") // Wrong
                        .lastChunk("chunk2") // Wrong
                        .startOffset(8)
                        .firstChunkStartOffset(8) // Wrong
                        .lastChunkStartOffset(9) // Wrong
                        .chunkCount(1) // Wrong
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1")
                        .lastChunk("chunk1")
                        .startOffset(8)
                        .firstChunkStartOffset(8)
                        .lastChunkStartOffset(8)
                        .chunkCount(2)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1")
                        .lastChunk("chunk1")
                        .startOffset(8)
                        .firstChunkStartOffset(8)
                        .lastChunkStartOffset(8)
                        .chunkCount(0)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .lastChunk("chunk1")
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1")
                        .lastChunk("chunk1")
                        .firstChunkStartOffset(2)
                        .lastChunkStartOffset(1)
                        .startOffset(2)
                        .chunkCount(1)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(10)
                        .firstChunk("chunk1")
                        .lastChunk("chunk1")
                        .firstChunkStartOffset(1)
                        .lastChunkStartOffset(2)
                        .startOffset(1)
                        .chunkCount(1)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(1)
                        .firstChunk("chunk1")
                        .lastChunk("chunk1")
                        .firstChunkStartOffset(2)
                        .lastChunkStartOffset(2)
                        .startOffset(2)
                        .chunkCount(1)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(3)
                        .firstChunk("chunk1")
                        .lastChunk("chunk1")
                        .firstChunkStartOffset(2)
                        .lastChunkStartOffset(3)
                        .startOffset(2)
                        .chunkCount(1)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(1) // Wrong
                        .startOffset(2)
                        .build(),
                SegmentMetadata.builder()
                        .name("Test")
                        .length(3) // Wrong
                        .startOffset(2)
                        .firstChunkStartOffset(2)
                        .lastChunkStartOffset(3)
                        .build(),
        };
        for (SegmentMetadata invalidData: invalidDataList) {
            AssertExtensions.assertThrows(invalidData.toString(),
                    () -> invalidData.checkInvariants(),
                    ex -> ex instanceof IllegalStateException);
        }
    }
}
